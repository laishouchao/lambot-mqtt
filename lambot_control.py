#!/usr/bin/env python3
"""
LAMBOT 扫地机本地MQTT控制客户端 (v2 - 自动发现版)
=================================================
通过本地MQTT协议直接控制岚豹扫地机，无需云端服务。

自动发现功能:
  1. 自动扫描局域网中开放19883端口的设备(MQTT Broker)
  2. 自动从MQTT主题中提取设备UUID
  3. 无需手动配置任何设备信息

协议信息（逆向工程获取）：
- Broker: 设备IP:19883 (无TLS)
- Username: Lambot
- Password: lambot123
- 发布主题: device/{uuid}/robot
- 订阅主题: device/{uuid}/app
- 数据格式: {"f": <功能码>, "p": <参数>}
"""
import paho.mqtt.client as mqtt
import json
import time
import uuid
import sys
import socket
import struct
import threading
import ipaddress
import re

# ============== 配置 ==============
# 如果已知设备IP，直接填写可跳过扫描步骤
# 留空则自动扫描局域网
KNOWN_BROKER_IP = ""  # 例如 "192.168.1.28"
KNOWN_DEVICE_UUID = ""  # 例如 "f5daee6e-b7ea-06a9-e10f-41ac24435030"

PORT = 19883
USERNAME = "Lambot"
PASSWORD = "lambot123"
CLIENT_ID = str(uuid.uuid4())

# ============== 全局变量 ==============
DEVICE_UUID = None
BROKER = None
CMD_TOPIC = None
STATUS_TOPIC = None
status = {}
connected = False
uuid_discovered = threading.Event()

# ============== MqttCommand 枚举 ==============
CMD = {
    'FORWARD': 20, 'BACKWARD': 21, 'TURN_LEFT': 22, 'TURN_RIGHT': 23,
    'STOP': 24, 'GO_HOME': 25, 'SWEEP': 26, 'SWEEP_SPOT': 27,
    'MOVE_TO': 28, 'UPDATE_VIRTUAL_WALL': 29, 'SYNC_SCHEDULE_TASK': 31,
    'SWEEP_TIME': 32, 'UPDATE_FIRMWARE': 33, 'FIRMWARE_INFO': 34,
    'START_MQTT_UPDATE': 40, 'HELLO': 41, 'ENTIRE_MAP': 42,
    'ROBO_TRACK': 53, 'CLEAR_MAP': 54, 'SWEEP_AREA': 55,
    'FIND_ME': 56, 'NETWORK_INFO': 57, 'GET_SWEEP_FAN_MODE': 58,
    'SET_SWEEP_FAN_MODE': 59, 'GET_KID_MODE': 60, 'SET_KID_MODE': 61,
    'GET_DO_NOT_DISTURB': 62, 'SET_DO_NOT_DISTURB': 63,
    'GET_SWEEP_REGION': 64, 'CREATE_SWEEP_REGION': 65,
    'DELETE_SWEEP_REGION': 66, 'UPDATE_SWEEP_REGION': 67,
    'START_SWEEP_REGION': 68, 'GET_SWEEPING_REGION': 69,
    'START_DRAWING_SWEEP': 70, 'GET_SWEEP_DRAWING': 71,
    'SET_MAP': 72, 'GET_SWEEP_DIRECTION': 77, 'SET_SWEEP_DIRECTION': 78,
    'SET_DEVICE_VOICE': 79, 'CHECK_SENSORS_STATUS': 84,
    'VOICE_VOLUME': 86, 'SWEEP_PARTITION_SIZE': 87,
    'SET_MOP_MODE': 101, 'SET_SMART_AREA_MODE': 102,
    'OPERATE_GOOGLE_HOME': 103, 'SET_TIMEZONE': 106,
    'START_AUTO_EXPLORING': 108, 'START_EDGE_SWEEPING': 109,
}

DATA_TYPE = {
    1: 'POSE', 2: 'CURRENT_ACTION', 3: 'BATTERY_PERCENTAGE',
    4: 'BATTERY_IS_CHARGING', 5: 'DC_IS_CONNECTED', 6: 'BOARD_TEMPERATURE',
    7: 'EXPLORE_MAP', 8: 'SWEEP_MAP', 9: 'VIRTUAL_WALLS',
    12: 'SWEEP_TIME', 13: 'FIRMWARE_PROCESS', 14: 'FIRMWARE_INFO',
    16: 'HELLO', 17: 'ENTIRE_EXPLORE_MAP', 18: 'ENTIRE_SWEEP_MAP',
    19: 'ROBOTRACK', 20: 'SWEEP_AREA', 21: 'DOCK_POSE',
    22: 'ROBOT_STATUS', 23: 'FIRMWARE_UPGRADE_SUCCESS', 24: 'NETWORK_INFO',
    25: 'SWEEP_FAN_MODE', 26: 'CHILD_SAFETY_LOCK', 27: 'SILENCE_MODE',
    28: 'SWEEP_REGION', 29: 'SWEEPING_REGION', 30: 'DRAWING_SWEEP_TRACK',
    31: 'SET_MAP_RESPONSE', 32: 'SWEEP_MOP_MODE', 50: 'SYSTEM_EVENT',
    54: 'SWEEP_DIRECTION', 55: 'VOICE_UPGRADE_PROCESS',
    56: 'VOICE_UPGRADE_SUCCESS', 57: 'DEVICE_VOICE_ID',
    58: 'SMART_PRESSURIZATION', 59: 'SENSORS_STATUS', 60: 'VOICE_VOLUME',
    61: 'SWEEP_PARTITION_SIZE', 62: 'WATER_BOX_ON', 66: 'DYEING_MAP',
    67: 'DEVICE_EVENTS', 100: 'DISCONNECT', 101: 'MOP_MODE',
    102: 'SMART_AREA_MODE', 103: 'GOOGLE_HOME_SERVICE',
}

DEVICE_STATES = {
    -5: '连接中', -4: '语音更新中', -3: '固件更新中', -2: '离线', -1: '在线',
    0: '空闲', 1: '充电中', 2: '电池已满', 3: '充电准备清扫',
    4: '回充中', 5: '回家中', 6: '清扫中', 7: '清扫暂停',
    8: '恢复定位', 9: '手动移动', 10: '未知',
    11: '定点清扫', 12: '定点清扫暂停', 13: '回家暂停',
    14: '区域清扫', 15: '区域清扫暂停', 16: '绘制清扫', 17: '绘制清扫暂停',
    18: '智能清扫', 19: '智能清扫暂停', 20: '自动探索', 21: '自动探索暂停',
    22: '沿边清扫', 23: '沿边清扫暂停',
}

FAN_MODES = {0: '标准', 1: '静音', 2: '强力', 3: '最大', 4: '继承'}
MOP_MODES = {0: '无', 1: '慢速', 2: '标准', 3: '快速', 4: '最大', 5: '继承'}


# ============== 网络扫描 ==============
def get_local_subnet():
    """获取本机所在的子网"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        # 假设/24子网
        parts = local_ip.split('.')
        return f"{parts[0]}.{parts[1]}.{parts[2]}.0/24"
    except:
        return None


def scan_port(ip, port, timeout=0.5):
    """扫描单个IP的指定端口"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        result = s.connect_ex((str(ip), port))
        s.close()
        return result == 0
    except:
        return False


def scan_lambot_devices(subnet_str, port=19883):
    """扫描局域网中开放MQTT端口的LAMBOT设备"""
    print(f"[*] 正在扫描子网 {subnet_str} 的 {port} 端口...")
    
    try:
        network = ipaddress.ip_network(subnet_str, strict=False)
    except:
        print("[✗] 无效的子网地址")
        return []
    
    found = []
    total = sum(1 for _ in network.hosts())
    scanned = 0
    
    def check_host(ip):
        nonlocal scanned
        if scan_port(str(ip), port):
            found.append(str(ip))
        scanned += 1
    
    # 多线程扫描
    threads = []
    for host in network.hosts():
        t = threading.Thread(target=check_host, args=(host,))
        threads.append(t)
        t.start()
        
        # 限制并发数
        if len(threads) >= 50:
            for t in threads:
                t.join()
            threads = []
            progress = scanned * 100 // total
            print(f"\r[*] 扫描进度: {progress}% ({scanned}/{total})", end='', flush=True)
    
    for t in threads:
        t.join()
    
    print(f"\r[*] 扫描完成: {scanned} 个主机, 发现 {len(found)} 个MQTT Broker")
    return found


def discover_broker():
    """自动发现MQTT Broker IP"""
    global BROKER
    
    if KNOWN_BROKER_IP:
        BROKER = KNOWN_BROKER_IP
        print(f"[*] 使用已知Broker: {BROKER}:{PORT}")
        return True
    
    print(f"\n{'='*55}")
    print(f"  LAMBOT 设备自动发现")
    print(f"{'='*55}")
    print(f"\n[*] 未配置设备IP，启动自动扫描...")
    
    subnet = get_local_subnet()
    if not subnet:
        print("[✗] 无法获取本机子网，请手动输入")
        manual = input("    输入设备IP (如 192.168.1.28): ").strip()
        if manual:
            BROKER = manual
            return True
        return False
    
    print(f"[*] 本机子网: {subnet}")
    devices = scan_lambot_devices(subnet)
    
    if not devices:
        print("[✗] 未发现LAMBOT设备，请确认:")
        print("    1. 设备已开机并连接到同一WiFi")
        print("    2. 设备IP在当前子网内")
        manual = input("\n    手动输入设备IP (如 192.168.1.28): ").strip()
        if manual:
            BROKER = manual
            return True
        return False
    
    if len(devices) == 1:
        BROKER = devices[0]
        print(f"[✓] 发现设备: {BROKER}:{PORT}")
        return True
    
    print(f"\n[*] 发现 {len(devices)} 个MQTT Broker:")
    for i, ip in enumerate(devices):
        print(f"    [{i+1}] {ip}:{PORT}")
    
    while True:
        choice = input(f"\n    选择设备 [1-{len(devices)}]: ").strip()
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(devices):
                BROKER = devices[idx]
                return True
        except:
            pass
        print("    无效选择，请重试")


# ============== UUID自动发现 ==============
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# 记录所有观察到的主题，用于提取UUID
observed_topics = []

def on_discover_message(client, userdata, msg):
    """监听所有主题，从topic中提取设备UUID"""
    global DEVICE_UUID
    topic = msg.topic
    observed_topics.append(topic)
    
    # 匹配 device/{uuid}/app 或 device/{uuid}/robot 格式
    match = re.match(r'device/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/', topic)
    if match:
        DEVICE_UUID = match.group(1)
        uuid_discovered.set()


def discover_device_uuid(broker_ip, port):
    """自动发现设备UUID - 多策略"""
    global DEVICE_UUID, CMD_TOPIC, STATUS_TOPIC
    
    if KNOWN_DEVICE_UUID:
        DEVICE_UUID = KNOWN_DEVICE_UUID
        CMD_TOPIC = f"device/{DEVICE_UUID}/robot"
        STATUS_TOPIC = f"device/{DEVICE_UUID}/app"
        print(f"[*] 使用已知UUID: {DEVICE_UUID}")
        return True
    
    print(f"[*] 正在从设备获取UUID...")
    
    disc_client = mqtt.Client(client_id=str(uuid.uuid4()), protocol=mqtt.MQTTv311)
    disc_client.username_pw_set(USERNAME, PASSWORD)
    
    def on_disc_connect(c, userdata, flags, rc):
        if rc == 0:
            print(f"[*] 已连接，订阅所有主题...")
            c.subscribe("#", 0)
            c.subscribe("$SYS/#", 0)
            # 主动发送唤醒消息触发设备响应
            print(f"[*] 发送唤醒消息...")
            c.publish("device/discover/robot", json.dumps({"f": 41}))
            c.publish("robot", json.dumps({"f": 41}))
            c.publish("app", json.dumps({"f": 41}))
            c.publish("command", json.dumps({"f": 41}))
            c.publish("lambot", json.dumps({"f": 40}))
    
    disc_client.on_connect = on_disc_connect
    disc_client.on_message = on_discover_message
    
    try:
        disc_client.connect(broker_ip, port, keepalive=60)
        disc_client.loop_start()
    except Exception as e:
        print(f"[✗] 连接失败: {e}")
        return False
    
    # 策略1: 等待设备主动发送心跳（最多30秒）
    print(f"[*] 策略1: 等待设备心跳 (最长30秒)...")
    found = uuid_discovered.wait(timeout=30)
    
    if not found:
        # 策略2: 再次发送唤醒消息
        print(f"\n[*] 策略2: 再次尝试唤醒设备...")
        disc_client.publish("device/discover/robot", json.dumps({"f": 40}))
        disc_client.publish("device/discover/robot", json.dumps({"f": 41}))
        found = uuid_discovered.wait(timeout=5)
    
    disc_client.loop_stop()
    disc_client.disconnect()
    
    if found and DEVICE_UUID:
        CMD_TOPIC = f"device/{DEVICE_UUID}/robot"
        STATUS_TOPIC = f"device/{DEVICE_UUID}/app"
        print(f"[✓] 自动发现UUID: {DEVICE_UUID}")
        return True
    
    # 策略3: 尝试从手机APP的SharedPreferences中提取UUID (需要ADB)
    print(f"\n[*] 策略3: 尝试通过ADB从手机APP提取UUID...")
    adb_uuid = try_adb_discover_uuid()
    if adb_uuid:
        DEVICE_UUID = adb_uuid
        CMD_TOPIC = f"device/{DEVICE_UUID}/robot"
        STATUS_TOPIC = f"device/{DEVICE_UUID}/app"
        print(f"[✓] 通过ADB发现UUID: {DEVICE_UUID}")
        return True
    
    # 显示调试信息
    if observed_topics:
        print(f"\n[!] 观察到的主题: {observed_topics[:10]}")
    
    print(f"\n[!] 自动发现UUID失败。可能原因:")
    print(f"    1. 设备未开机或未联网")
    print(f"    2. 设备MQTT Broker未运行")
    print(f"    3. 设备需要APP先触发才会发送心跳")
    print(f"\n[*] 解决方法:")
    print(f"    方法A: 打开手机LAMBOT APP连接一次设备，然后重新运行此脚本")
    print(f"    方法B: 手动输入设备UUID (格式: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)")
    
    manual = input("\n    输入UUID (回车退出): ").strip()
    if manual:
        DEVICE_UUID = manual
        CMD_TOPIC = f"device/{DEVICE_UUID}/robot"
        STATUS_TOPIC = f"device/{DEVICE_UUID}/app"
        return True
    return False


def try_adb_discover_uuid():
    """尝试通过ADB从手机APP的SharedPreferences中提取设备UUID"""
    import subprocess
    
    adb_paths = [
        r"D:\03_PenTestTools\abd\adb.exe",
        "adb",
    ]
    
    adb_cmd = None
    for path in adb_paths:
        try:
            result = subprocess.run([path, "devices"], capture_output=True, text=True, timeout=3)
            if "device" in result.stdout and "List of devices" in result.stdout:
                adb_cmd = path
                break
        except:
            continue
    
    if not adb_cmd:
        print(f"    ADB不可用或未连接手机")
        return None
    
    print(f"    ADB已连接，正在读取APP数据...")
    
    try:
        # 尝试读取SharedPreferences
        result = subprocess.run(
            [adb_cmd, "shell", "su -c",
             "cat /data/data/ai.lambot.android.vacuum/shared_prefs/com.slamtec.android.robohome_preferences.xml"],
            capture_output=True, text=True, timeout=5
        )
        
        if result.returncode == 0 and result.stdout:
            content = result.stdout
            # 搜索UUID格式的字符串
            uuid_match = re.search(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', content)
            if uuid_match:
                return uuid_match.group(0)
        
        # 尝试从robohome数据库中提取
        result = subprocess.run(
            [adb_cmd, "shell", "su -c",
             "cat /data/data/ai.lambot.android.vacuum/shared_prefs/ai.lambot.android.vacuum_preferences.xml"],
            capture_output=True, text=True, timeout=5
        )
        
        if result.returncode == 0 and result.stdout:
            content = result.stdout
            # 查找deviceId字段
            id_match = re.search(r'name="device_id"[^>]*>([^<]+)<', content)
            if id_match:
                device_id = id_match.group(1)
                print(f"    找到device_id: {device_id}")
            
            uuid_match = re.search(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', content)
            if uuid_match:
                return uuid_match.group(0)
        
        # 尝试grep搜索所有SharedPreferences文件
        result = subprocess.run(
            [adb_cmd, "shell", "su -c",
             "grep -r '[0-9a-f]\\{8\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{12\\}' /data/data/ai.lambot.android.vacuum/shared_prefs/ 2>/dev/null"],
            capture_output=True, text=True, timeout=10
        )
        
        if result.returncode == 0 and result.stdout:
            # 提取所有UUID，排除已知的user_key
            uuids = re.findall(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', result.stdout)
            # 过滤掉user_key和常见的非设备UUID
            known_non_device = ['eefe39bc-8740-42e6-ad6b-d3b031a671a1']  # user_key
            device_uuids = [u for u in set(uuids) if u not in known_non_device]
            
            if device_uuids:
                if len(device_uuids) == 1:
                    return device_uuids[0]
                else:
                    print(f"    找到多个UUID:")
                    for i, u in enumerate(device_uuids):
                        print(f"      [{i+1}] {u}")
                    choice = input(f"    选择 [1-{len(device_uuids)}]: ").strip()
                    try:
                        return device_uuids[int(choice) - 1]
                    except:
                        pass
        
        print(f"    ADB未找到设备UUID")
        return None
        
    except Exception as e:
        print(f"    ADB查询失败: {e}")
        return None


# ============== MQTT回调 ==============
def on_connect(client, userdata, flags, rc):
    global connected
    if rc == 0:
        connected = True
        print(f"\n[✓] 已连接到 {BROKER}:{PORT}")
        print(f"[✓] 设备UUID: {DEVICE_UUID}")
        # 订阅设备主题
        client.subscribe(f"device/{DEVICE_UUID}/#", 0)
    else:
        rc_names = {1: '协议版本错误', 2: '客户端ID无效', 3: '服务器不可用', 4: '用户名或密码错误', 5: '未授权'}
        print(f"\n[✗] 连接失败: RC={rc} ({rc_names.get(rc, '未知')})")


def on_message(client, userdata, msg):
    global status
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
        f_val = payload.get('f')
        p_val = payload.get('p')
        type_name = DATA_TYPE.get(f_val, f'CMD:{f_val}')
        
        # 跳过心跳消息
        if f_val in [16, 40, 41]:
            return
        
        # === 电池/充电 ===
        if f_val == 3 and isinstance(p_val, (int, float)):
            status['battery'] = p_val
            print(f"  [电量] {p_val}%")
        elif f_val == 4:
            status['charging'] = p_val
            print(f"  [充电] {'是' if p_val else '否'}")
        elif f_val == 5:
            status['dc_connected'] = p_val
            print(f"  [充电连接] {'是' if p_val else '否'}")
        
        # === 设备状态 ===
        elif f_val == 22 and isinstance(p_val, (int, float)):
            status['state'] = p_val
            state_name = DEVICE_STATES.get(p_val, f'未知({p_val})')
            print(f"  [设备状态] {state_name}")
        
        # === 吸力/拖地模式 ===
        elif f_val == 25 and isinstance(p_val, (int, float)):
            status['fan_mode'] = p_val
            print(f"  [吸力] {FAN_MODES.get(p_val, f'未知({p_val})')}")
        elif f_val == 32 and isinstance(p_val, (int, float)):
            status['mop_mode'] = p_val
            print(f"  [拖地] {MOP_MODES.get(p_val, f'未知({p_val})')}")
        
        # === 温度/面积/时间 ===
        elif f_val == 6:
            status['temperature'] = p_val
            print(f"  [温度] {p_val}°C")
        elif f_val == 20:
            status['area'] = p_val
            print(f"  [面积] {p_val}")
        elif f_val == 12:
            status['sweep_time'] = p_val
            print(f"  [时间] {p_val}")
        
        # === 位置/动作 ===
        elif f_val == 1:
            status['pose'] = p_val
            print(f"  [位置] {p_val}")
        elif f_val == 2:
            status['action'] = p_val
            print(f"  [动作] {p_val}")
        elif f_val == 21:
            status['dock_pose'] = p_val
            print(f"  [充电桩位置] {p_val}")
        
        # === 网络/固件 ===
        elif f_val == 24:
            status['network'] = p_val
            if isinstance(p_val, dict):
                print(f"  [网络] SSID={p_val.get('ssid','?')} IP={p_val.get('ip','?')} MAC={p_val.get('mac','?')}")
            else:
                print(f"  [网络] {p_val}")
        elif f_val == 14:
            status['firmware'] = p_val
            if isinstance(p_val, dict):
                print(f"  [固件] {p_val.get('v', p_val)}")
            else:
                print(f"  [固件] {p_val}")
        
        # === 儿童锁/勿扰/静音 ===
        elif f_val == 26:
            status['child_lock'] = p_val
            if isinstance(p_val, dict):
                print(f"  [儿童锁] {'开启' if p_val.get('isLock') else '关闭'}")
            else:
                print(f"  [儿童锁] {'开启' if p_val else '关闭'}")
        elif f_val == 27:
            status['silence_mode'] = p_val
            if isinstance(p_val, dict):
                print(f"  [静音模式] {'开启' if p_val.get('enabled') else '关闭'}")
            else:
                print(f"  [静音模式] {'开启' if p_val else '关闭'}")
        
        # === 清扫方向 ===
        elif f_val == 54:
            status['sweep_direction'] = p_val
            print(f"  [清扫方向] {p_val}")
        
        # === 语音 ===
        elif f_val == 57:
            status['voice_id'] = p_val
            print(f"  [语音ID] {p_val}")
        elif f_val == 60:
            status['voice_volume'] = p_val
            print(f"  [语音音量] {p_val}")
        
        # === 水箱 ===
        elif f_val == 62:
            status['water_box'] = p_val
            if isinstance(p_val, dict):
                print(f"  [水箱] {'已安装' if p_val.get('isWaterBoxOn') else '未安装'}")
            else:
                print(f"  [水箱] {'已安装' if p_val else '未安装'}")
        
        # === 虚拟墙 ===
        elif f_val == 9:
            status['virtual_walls'] = p_val
            print(f"  [虚拟墙] {json.dumps(p_val, ensure_ascii=False)[:100]}")
        
        # === 清扫区域 ===
        elif f_val == 28:
            status['sweep_regions'] = p_val
            print(f"  [清扫区域] {json.dumps(p_val, ensure_ascii=False)[:100]}")
        elif f_val == 29:
            status['sweeping_region'] = p_val
            print(f"  [当前清扫区域] {json.dumps(p_val, ensure_ascii=False)[:100]}")
        
        # === 地图 ===
        elif f_val == 7:
            print(f"  [探索地图] 数据已接收 ({len(str(p_val))} bytes)")
        elif f_val == 8:
            print(f"  [扫地地图] 数据已接收 ({len(str(p_val))} bytes)")
        elif f_val == 17:
            print(f"  [完整探索地图] 数据已接收 ({len(str(p_val))} bytes)")
        elif f_val == 18:
            print(f"  [完整扫地地图] 数据已接收 ({len(str(p_val))} bytes)")
        elif f_val == 31:
            print(f"  [设置地图响应] {'成功' if p_val else '失败'}")
        
        # === 传感器 ===
        elif f_val == 59:
            status['sensors'] = p_val
            if isinstance(p_val, dict):
                masks = p_val.get('sensor_masks', [])
                events = p_val.get('events', [])
                print(f"  [传感器] masks={masks}, events={events}")
            else:
                print(f"  [传感器] {p_val}")
        
        # === 事件 ===
        elif f_val == 50:
            print(f"  [系统事件] {json.dumps(p_val, ensure_ascii=False)[:100]}")
        elif f_val == 67:
            print(f"  [设备事件] {json.dumps(p_val, ensure_ascii=False)[:100]}")
        
        # === 机器人轨迹 ===
        elif f_val == 19:
            print(f"  [轨迹] 数据已接收")
        
        # === 其他 ===
        elif f_val == 58:
            status['smart_pressurization'] = p_val
            print(f"  [智能加压] {p_val}")
        elif f_val == 66:
            print(f"  [染色地图] {json.dumps(p_val, ensure_ascii=False)[:100]}")
        elif f_val == 100:
            print(f"  [断开连接] {p_val}")
        elif f_val == 102:
            print(f"  [智能区域模式] {p_val}")
        elif f_val == 103:
            print(f"  [Google Home] {json.dumps(p_val, ensure_ascii=False)[:100]}")
        else:
            p_str = json.dumps(p_val, ensure_ascii=False)[:80] if p_val else '(无数据)'
            print(f"  [{type_name}] {p_str}")
    except:
        pass


def send_command(cmd_name, param=None):
    """发送MQTT命令到设备"""
    if cmd_name not in CMD:
        print(f"[✗] 未知命令: {cmd_name}")
        return
    
    cmd_code = CMD[cmd_name]
    payload = {"f": cmd_code}
    if param is not None:
        payload["p"] = param
    
    msg = json.dumps(payload)
    result = client.publish(CMD_TOPIC, msg)
    print(f"[→] 发送: {cmd_name}({cmd_code}) -> {msg}")
    return result


def print_status():
    """打印当前状态"""
    print(f"\n{'='*55}")
    print(f"  LAMBOT 扫地机 状态总览")
    print(f"{'='*55}")
    
    # 电量/充电
    if 'battery' in status:
        bat = status['battery']
        icon = '🔋' if bat > 50 else '🪫' if bat > 20 else '⚠️'
        print(f"  {icon} 电量: {bat}%")
    if 'charging' in status:
        print(f"  🔌 充电中: {'是' if status['charging'] else '否'}")
    
    # 设备状态
    if 'state' in status:
        state_name = DEVICE_STATES.get(status['state'], f"未知({status['state']})")
        print(f"  🤖 状态: {state_name}")
    
    # 吸力/拖地
    if 'fan_mode' in status:
        mode_name = FAN_MODES.get(status['fan_mode'], f"未知({status['fan_mode']})")
        print(f"  🌀 吸力: {mode_name}")
    if 'mop_mode' in status:
        mode_name = MOP_MODES.get(status['mop_mode'], f"未知({status['mop_mode']})")
        print(f"  🧹 拖地: {mode_name}")
    
    # 温度/面积/时间
    if 'temperature' in status:
        print(f"  🌡️  温度: {status['temperature']}°C")
    if 'area' in status:
        print(f"  📐 面积: {status['area']}")
    if 'sweep_time' in status:
        print(f"  ⏱️  时间: {status['sweep_time']}")
    
    # 位置
    if 'pose' in status:
        print(f"  📍 位置: {status['pose']}")
    if 'dock_pose' in status:
        print(f"  🏠 充电桩: {status['dock_pose']}")
    
    # 网络
    if 'network' in status:
        net = status['network']
        if isinstance(net, dict):
            print(f"  📶 WiFi: {net.get('ssid','?')} ({net.get('ip','?')})")
    
    # 固件
    if 'firmware' in status:
        fw = status['firmware']
        if isinstance(fw, dict):
            print(f"  📦 固件: {fw.get('v', fw)}")
    
    # 水箱
    if 'water_box' in status:
        wb = status['water_box']
        is_on = wb.get('isWaterBoxOn') if isinstance(wb, dict) else wb
        print(f"  💧 水箱: {'已安装' if is_on else '未安装'}")
    
    # 儿童锁/静音
    if 'child_lock' in status:
        cl = status['child_lock']
        is_on = cl.get('isLock') if isinstance(cl, dict) else cl
        print(f"  🔒 儿童锁: {'开启' if is_on else '关闭'}")
    if 'silence_mode' in status:
        sm = status['silence_mode']
        is_on = sm.get('enabled') if isinstance(sm, dict) else sm
        print(f"  🔇 静音: {'开启' if is_on else '关闭'}")
    
    print(f"{'='*55}")


def print_help():
    """打印帮助信息"""
    print(f"""
{'='*60}
  LAMBOT 扫地机 MQTT 控制客户端 v3 (全功能版)
{'='*60}
  
  【基本控制】
    sweep           开始清扫
    stop            停止
    home            回充电桩
    find            查找机器人（发出声音）
    explore         开始自动探索
    edge            开始沿边清扫
    spot            定点清扫
  
  【吸力控制】
    fan             获取当前吸力模式
    fan_normal      设置吸力为标准
    fan_silent      设置吸力为静音
    fan_high        设置吸力为强力
    fan_max         设置吸力为最大
  
  【拖地控制】
    mop             获取当前拖地模式
    mop_off         关闭拖地
    mop_slow        拖地慢速
    mop_normal      拖地标准
    mop_fast        拖地快速
    mop_max         拖地最大
  
  【儿童安全锁】
    kid             查询儿童锁状态
    kid_on          开启儿童锁
    kid_off         关闭儿童锁
  
  【勿扰/静音模式】
    dnd             查询勿扰模式
    dnd_on          开启勿扰模式
    dnd_off         关闭勿扰模式
    silence         查询静音模式
  
  【清扫方向】
    dir             查询清扫方向
    dir_zigzag      设置为弓字形清扫
    dir_wall        设置为沿墙清扫
  
  【语音设置】
    voice_query     查询语音ID
    voice_vol       查询语音音量
  
  【时区设置】
    tz              查询当前时区
    tz_cn           设置为中国时区 (UTC+8)
    tz_jp           设置为日本时区 (UTC+9)
    tz_utc          设置为UTC时区
    tz <IANA>       设置自定义时区 (如 tz Asia/Shanghai)
  
  【虚拟墙管理】
    vwall_query     查询虚拟墙
    vwall <JSON>    设置虚拟墙 (如 vwall {{"points":[...]}})
  
  【区域管理】
    regions         查询清扫区域
    region_start    开始区域清扫
    region_create   创建清扫区域 (需JSON参数)
    region_delete   删除清扫区域 (需ID)
    region_update   更新清扫区域 (需JSON参数)
  
  【地图管理】
    map             请求完整地图
    map_clear       清除地图
    map_set         设置地图 (需JSON参数)
  
  【定时任务】
    schedule        查询定时任务
    schedule_sync   同步定时任务 (需JSON参数)
  
  【传感器状态】
    sensors         查询传感器状态
    sensor_check    检查传感器状态
  
  【固件管理】
    firmware        查询固件信息
    fw_update       更新固件 (需JSON参数)
  
  【状态查询】
    status          显示所有状态
    battery         查询电量
    network         查询网络信息
    fan_query       查询吸力模式
  
  【手动控制】
    forward         前进
    backward        后退
    left            左转
    right           右转
  
  【系统】
    info            显示设备信息
    help            显示帮助
    quit            退出
  
  【高级】
    json {{"f":26}}        直接发送原始MQTT命令
    json {{"f":59,"p":2}}  带参数的原始命令
{'='*60}
""")


def main():
    global client
    
    print(f"""
╔═══════════════════════════════════════════════════════╗
║       LAMBOT 岚豹扫地机 MQTT 控制客户端 v3          		║
║       如果无法获取UUID，请开启手机APP后再试          		║
║           自动发现 · 全功能控制 · 一键连接           		║
╚═══════════════════════════════════════════════════════╝
""")
    
    # 步骤1: 发现Broker IP
    if not discover_broker():
        print("[✗] 未能发现设备，退出")
        return
    
    # 步骤2: 发现设备UUID
    if not discover_device_uuid(BROKER, PORT):
        print("[✗] 未能获取设备UUID，退出")
        return
    
    # 步骤3: 建立控制连接
    print(f"\n[*] 正在建立控制连接...")
    print(f"    Broker: {BROKER}:{PORT}")
    print(f"    UUID:   {DEVICE_UUID}")
    print(f"    命令:   {CMD_TOPIC}")
    print(f"    状态:   {STATUS_TOPIC}")
    
    client = mqtt.Client(client_id=CLIENT_ID, protocol=mqtt.MQTTv311)
    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        client.connect(BROKER, PORT, keepalive=60)
        client.loop_start()
    except Exception as e:
        print(f"[✗] 连接失败: {e}")
        return
    
    for _ in range(10):
        if connected:
            break
        time.sleep(0.5)
    
    if not connected:
        print("[✗] 连接超时")
        return
    
    time.sleep(1)
    send_command('START_MQTT_UPDATE')
    time.sleep(2)
    
    print_help()
    
    while True:
        try:
            cmd = input("\n> ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            break
        
        if not cmd:
            continue
        
        if cmd in ('quit', 'exit'):
            break
        elif cmd == 'help':
            print_help()
        elif cmd == 'info':
            print(f"\n  Broker: {BROKER}:{PORT}")
            print(f"  UUID:   {DEVICE_UUID}")
            print(f"  用户:   {USERNAME}")
            print(f"  Client: {CLIENT_ID}")
        # ===== 基本控制 =====
        elif cmd == 'sweep':
            send_command('SWEEP')
        elif cmd == 'stop':
            send_command('STOP')
        elif cmd == 'home':
            send_command('GO_HOME')
        elif cmd == 'find':
            send_command('FIND_ME')
        elif cmd == 'explore':
            send_command('START_AUTO_EXPLORING')
        elif cmd == 'edge':
            send_command('START_EDGE_SWEEPING')
        elif cmd == 'spot':
            send_command('SWEEP_SPOT')
        
        # ===== 手动控制 =====
        elif cmd == 'forward':
            send_command('FORWARD')
        elif cmd == 'backward':
            send_command('BACKWARD')
        elif cmd == 'left':
            send_command('TURN_LEFT')
        elif cmd == 'right':
            send_command('TURN_RIGHT')
        
        # ===== 吸力控制 =====
        elif cmd in ('fan', 'fan_query'):
            send_command('GET_SWEEP_FAN_MODE')
        elif cmd == 'fan_normal':
            send_command('SET_SWEEP_FAN_MODE', 0)
        elif cmd == 'fan_silent':
            send_command('SET_SWEEP_FAN_MODE', 1)
        elif cmd == 'fan_high':
            send_command('SET_SWEEP_FAN_MODE', 2)
        elif cmd == 'fan_max':
            send_command('SET_SWEEP_FAN_MODE', 3)
        
        # ===== 拖地控制 =====
        elif cmd == 'mop':
            send_command('START_MQTT_UPDATE')
        elif cmd == 'mop_off':
            send_command('SET_MOP_MODE', 0)
        elif cmd == 'mop_slow':
            send_command('SET_MOP_MODE', 1)
        elif cmd == 'mop_normal':
            send_command('SET_MOP_MODE', 2)
        elif cmd == 'mop_fast':
            send_command('SET_MOP_MODE', 3)
        elif cmd == 'mop_max':
            send_command('SET_MOP_MODE', 4)
        
        # ===== 儿童安全锁 =====
        elif cmd == 'kid':
            send_command('GET_KID_MODE')
        elif cmd == 'kid_on':
            send_command('SET_KID_MODE', True)
        elif cmd == 'kid_off':
            send_command('SET_KID_MODE', False)
        
        # ===== 勿扰/静音模式 =====
        elif cmd == 'dnd':
            send_command('GET_DO_NOT_DISTURB')
        elif cmd == 'dnd_on':
            send_command('SET_DO_NOT_DISTURB', True)
        elif cmd == 'dnd_off':
            send_command('SET_DO_NOT_DISTURB', False)
        elif cmd == 'silence':
            send_command('START_MQTT_UPDATE')
            time.sleep(1)
            if 'silence_mode' in status:
                val = status['silence_mode']
                is_on = val.get('enabled') if isinstance(val, dict) else val
                print(f"  [静音模式] {'开启' if is_on else '关闭'}")
        
        # ===== 清扫方向 =====
        elif cmd == 'dir':
            send_command('GET_SWEEP_DIRECTION')
        elif cmd == 'dir_zigzag':
            send_command('SET_SWEEP_DIRECTION', 0)
            print("  [设置] 弓字形清扫")
        elif cmd == 'dir_wall':
            send_command('SET_SWEEP_DIRECTION', 1)
            print("  [设置] 沿墙清扫")
        
        # ===== 语音设置 =====
        elif cmd == 'voice_query':
            send_command('GET_VOICE_ID')
        elif cmd == 'voice_vol':
            send_command('VOICE_VOLUME')
        
        # ===== 时区设置 =====
        elif cmd == 'tz':
            print("  [提示] 使用 tz_cn/tz_jp/tz_utc 设置时区，或 tz <IANA> 自定义")
        elif cmd == 'tz_cn':
            send_command('SET_TIMEZONE', 'Asia/Shanghai')
            print("  [设置] 时区: Asia/Shanghai (UTC+8)")
        elif cmd == 'tz_jp':
            send_command('SET_TIMEZONE', 'Asia/Tokyo')
            print("  [设置] 时区: Asia/Tokyo (UTC+9)")
        elif cmd == 'tz_utc':
            send_command('SET_TIMEZONE', 'UTC')
            print("  [设置] 时区: UTC")
        elif cmd.startswith('tz '):
            tz_val = cmd[3:].strip()
            if tz_val:
                send_command('SET_TIMEZONE', tz_val)
                print(f"  [设置] 时区: {tz_val}")
            else:
                print("  [错误] 请指定时区，如: tz Asia/Shanghai")
        
        # ===== 虚拟墙管理 =====
        elif cmd == 'vwall_query':
            send_command('START_MQTT_UPDATE')
            time.sleep(1)
            if 'virtual_walls' in status:
                print(f"  [虚拟墙] {json.dumps(status['virtual_walls'], ensure_ascii=False)}")
        elif cmd.startswith('vwall '):
            try:
                vwall_data = json.loads(cmd[6:].strip())
                send_command('UPDATE_VIRTUAL_WALL', vwall_data)
                print(f"  [设置] 虚拟墙已发送")
            except json.JSONDecodeError as e:
                print(f"  [错误] JSON格式错误: {e}")
                print(f"  [示例] vwall {{\"points\":[{{\"x\":0,\"y\":0}},{{\"x\":1,\"y\":1}}]}}")
        
        # ===== 区域管理 =====
        elif cmd == 'regions':
            send_command('GET_SWEEP_REGION')
        elif cmd == 'region_start':
            send_command('START_SWEEP_REGION')
            print("  [执行] 开始区域清扫")
        elif cmd.startswith('region_create '):
            try:
                region_data = json.loads(cmd[14:].strip())
                send_command('CREATE_SWEEP_REGION', region_data)
                print(f"  [创建] 区域已发送")
            except json.JSONDecodeError as e:
                print(f"  [错误] JSON格式错误: {e}")
        elif cmd.startswith('region_delete '):
            region_id = cmd[14:].strip()
            send_command('DELETE_SWEEP_REGION', region_id)
            print(f"  [删除] 区域ID: {region_id}")
        elif cmd.startswith('region_update '):
            try:
                region_data = json.loads(cmd[14:].strip())
                send_command('UPDATE_SWEEP_REGION', region_data)
                print(f"  [更新] 区域已发送")
            except json.JSONDecodeError as e:
                print(f"  [错误] JSON格式错误: {e}")
        
        # ===== 地图管理 =====
        elif cmd == 'map':
            send_command('ENTIRE_MAP')
        elif cmd == 'map_clear':
            confirm = input("  [确认] 清除地图将删除所有保存的地图数据，继续? (y/N): ").strip().lower()
            if confirm == 'y':
                send_command('CLEAR_MAP')
                print("  [执行] 地图清除命令已发送")
            else:
                print("  [取消] 已取消")
        elif cmd.startswith('map_set '):
            try:
                map_data = json.loads(cmd[8:].strip())
                send_command('SET_MAP', map_data)
                print(f"  [设置] 地图数据已发送")
            except json.JSONDecodeError as e:
                print(f"  [错误] JSON格式错误: {e}")
        
        # ===== 定时任务 =====
        elif cmd == 'schedule':
            print("  [提示] 定时任务数据存储在APP端，设备端通过同步命令更新")
            send_command('START_MQTT_UPDATE')
        elif cmd.startswith('schedule_sync '):
            try:
                schedule_data = json.loads(cmd[14:].strip())
                send_command('SYNC_SCHEDULE_TASK', schedule_data)
                print(f"  [同步] 定时任务已发送")
            except json.JSONDecodeError as e:
                print(f"  [错误] JSON格式错误: {e}")
        
        # ===== 传感器 =====
        elif cmd in ('sensors', 'sensor_check'):
            send_command('CHECK_SENSORS_STATUS')
        
        # ===== 固件管理 =====
        elif cmd == 'firmware':
            send_command('FIRMWARE_INFO')
        elif cmd.startswith('fw_update '):
            try:
                fw_data = json.loads(cmd[10:].strip())
                send_command('UPDATE_FIRMWARE', fw_data)
                print(f"  [更新] 固件更新命令已发送")
                print(f"  [警告] 固件更新期间请勿断电!")
            except json.JSONDecodeError as e:
                print(f"  [错误] JSON格式错误: {e}")
        
        # ===== 状态查询 =====
        elif cmd == 'status':
            send_command('START_MQTT_UPDATE')
            time.sleep(3)
            print_status()
        elif cmd == 'battery':
            send_command('START_MQTT_UPDATE')
            time.sleep(1)
        elif cmd == 'network':
            send_command('NETWORK_INFO')
        
        # ===== 高级: 原始JSON =====
        elif cmd.startswith('json '):
            try:
                json_str = cmd[5:].strip()
                data = json.loads(json_str)
                payload = json.dumps(data)
                client.publish(CMD_TOPIC, payload)
                print(f"[→] 发送原始JSON: {payload}")
            except json.JSONDecodeError as e:
                print(f"[✗] JSON解析错误: {e}")
        else:
            print(f"[?] 未知命令: {cmd} (输入 help 查看帮助)")
    
    print("\n[*] 断开连接...")
    client.loop_stop()
    client.disconnect()
    print("[*] 已退出")


if __name__ == '__main__':
    main()
