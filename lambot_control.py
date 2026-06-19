#!/usr/bin/env python3
"""
LAMBOT 扫地机本地MQTT控制客户端
================================
通过本地MQTT协议直接控制岚豹扫地机，无需云端服务。

协议信息（逆向工程获取）：
- Broker: 192.168.1.28:19883 (无TLS)
- Username: Lambot
- Password: lambot123
- 发布主题: device/{uuid}/robot
- 订阅主题: device/{uuid}/app
- 数据格式: {"f": <功能码>, "p": <参数>}

功能码映射 (MqttCommand):
  20=前进, 21=后退, 22=左转, 23=右转, 24=停止, 25=回充,
  26=清扫, 27=定点清扫, 28=移动到, 40=请求更新, 41=心跳,
  42=请求地图, 56=查找机器人, 58=获取吸力, 59=设置吸力,
  68=区域清扫, 101=设置拖地模式, 108=自动探索, 109=沿边清扫

数据类型映射 (MqttDataType):
  1=位置, 2=当前动作, 3=电量, 4=充电状态, 5=充电连接,
  6=主板温度, 7=探索地图, 8=扫地地图, 9=虚拟墙, 12=清扫时间,
  16=心跳, 20=清扫面积, 21=充电桩位置, 22=机器人状态,
  25=吸力模式, 32=拖地模式, 50=系统事件, 62=水箱状态

设备状态 (DeviceState):
  0=空闲, 1=充电中, 2=电池满, 4=回充中, 6=清扫中, 7=暂停,
  11=定点清扫, 14=区域清扫, 18=智能清扫, 20=自动探索, 22=沿边清扫

吸力模式 (SweepFanMode): 0=标准, 1=静音, 2=强力, 3=最大
拖地模式 (MopMode): 0=无, 1=慢速, 2=标准, 3=快速, 4=最大
"""
import paho.mqtt.client as mqtt
import json
import time
import uuid
import sys
import signal
import os

# ============== 配置 ==============
BROKER = "192.168.1.xx" #扫地机内网IP，从路由器获取
PORT = 19883
USERNAME = "Lambot"
PASSWORD = "lambot123"
DEVICE_UUID = "f5daee6e-b7ea-06a9-e10f-000000000000" #自己扫地机的UUID
CLIENT_ID = str(uuid.uuid4())

# 命令主题 (APP → 设备)
CMD_TOPIC = f"device/{DEVICE_UUID}/robot"
# 状态主题 (设备 → APP)
STATUS_TOPIC = f"device/{DEVICE_UUID}/app"

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

# MqttDataType 枚举 (设备推送的数据类型)
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

# 设备状态枚举
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

# 吸力模式
FAN_MODES = {0: '标准', 1: '静音', 2: '强力', 3: '最大', 4: '继承'}
# 拖地模式
MOP_MODES = {0: '无', 1: '慢速', 2: '标准', 3: '快速', 4: '最大', 5: '继承'}

# ============== 状态存储 ==============
status = {}
connected = False

def on_connect(client, userdata, flags, rc):
    global connected
    if rc == 0:
        connected = True
        print(f"\n[✓] 已连接到 {BROKER}:{PORT}")
        # 订阅设备状态
        client.subscribe(f"device/{DEVICE_UUID}/#", 0)
        client.subscribe(f"device/#", 0)
    else:
        print(f"\n[✗] 连接失败: RC={rc}")

def on_message(client, userdata, msg):
    global status
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
        f_val = payload.get('f')
        p_val = payload.get('p')
        topic = msg.topic
        
        type_name = DATA_TYPE.get(f_val, f'CMD:{f_val}')
        
        # 解析关键状态
        if f_val == 3 and isinstance(p_val, (int, float)):
            status['battery'] = p_val
            print(f"  [状态] 电量: {p_val}%")
        elif f_val == 4:
            status['charging'] = p_val
            print(f"  [状态] 充电: {'是' if p_val else '否'}")
        elif f_val == 22 and isinstance(p_val, (int, float)):
            status['state'] = p_val
            state_name = DEVICE_STATES.get(p_val, f'未知({p_val})')
            print(f"  [状态] 设备状态: {state_name}")
        elif f_val == 25 and isinstance(p_val, (int, float)):
            status['fan_mode'] = p_val
            mode_name = FAN_MODES.get(p_val, f'未知({p_val})')
            print(f"  [状态] 吸力模式: {mode_name}")
        elif f_val == 32 and isinstance(p_val, (int, float)):
            status['mop_mode'] = p_val
            mode_name = MOP_MODES.get(p_val, f'未知({p_val})')
            print(f"  [状态] 拖地模式: {mode_name}")
        elif f_val == 6:
            status['temperature'] = p_val
            print(f"  [状态] 主板温度: {p_val}°C")
        elif f_val == 1:
            status['pose'] = p_val
            print(f"  [状态] 位置: {p_val}")
        elif f_val == 20:
            status['area'] = p_val
            print(f"  [状态] 清扫面积: {p_val}")
        elif f_val == 12:
            status['sweep_time'] = p_val
            print(f"  [状态] 清扫时间: {p_val}")
        elif f_val == 2:
            status['action'] = p_val
            print(f"  [状态] 当前动作: {p_val}")
        elif f_val == 50:
            print(f"  [事件] {p_val}")
        elif f_val == 67:
            print(f"  [设备事件] {p_val}")
        elif f_val == 24:
            status['network'] = p_val
            print(f"  [状态] 网络: {p_val}")
        elif f_val not in [16, 40, 41]:  # 忽略心跳
            print(f"  [{type_name}] {json.dumps(p_val, ensure_ascii=False) if p_val else '(无数据)'}")
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
    print(f"\n{'='*50}")
    print(f"  LAMBOT 扫地机状态")
    print(f"{'='*50}")
    if 'battery' in status:
        print(f"  电量: {status['battery']}%")
    if 'charging' in status:
        print(f"  充电中: {'是' if status['charging'] else '否'}")
    if 'state' in status:
        state_name = DEVICE_STATES.get(status['state'], f"未知({status['state']})")
        print(f"  状态: {state_name}")
    if 'fan_mode' in status:
        mode_name = FAN_MODES.get(status['fan_mode'], f"未知({status['fan_mode']})")
        print(f"  吸力: {mode_name}")
    if 'mop_mode' in status:
        mode_name = MOP_MODES.get(status['mop_mode'], f"未知({status['mop_mode']})")
        print(f"  拖地: {mode_name}")
    if 'temperature' in status:
        print(f"  温度: {status['temperature']}°C")
    if 'area' in status:
        print(f"  清扫面积: {status['area']}")
    if 'sweep_time' in status:
        print(f"  清扫时间: {status['sweep_time']}")
    if 'pose' in status:
        print(f"  位置: {status['pose']}")
    print(f"{'='*50}")

def print_help():
    """打印帮助信息"""
    print(f"""
{'='*55}
  LAMBOT 扫地机 MQTT 控制客户端
{'='*55}
  
  基本控制:
    sweep       开始清扫
    stop        停止
    home        回充电桩
    find        查找机器人（发出声音）
    explore     开始自动探索
    edge        开始沿边清扫
    spot        定点清扫
  
  吸力控制:
    fan         获取当前吸力模式
    fan_normal  设置吸力为标准
    fan_silent  设置吸力为静音
    fan_high    设置吸力为强力
    fan_max     设置吸力为最大
  
  拖地控制:
    mop         获取当前拖地模式
    mop_off     关闭拖地
    mop_slow    拖地慢速
    mop_normal  拖地标准
    mop_fast    拖地快速
    mop_max     拖地最大
  
  状态查询:
    status      显示当前状态
    battery     查询电量
    map         请求完整地图
    sensors     查询传感器状态
    network     查询网络信息
    firmware    查询固件信息
    fan_query   查询吸力模式
    regions     查询清扫区域
  
  手动控制:
    forward     前进
    backward    后退
    left        左转
    right       右转
  
  系统:
    help        显示帮助
    quit        退出
  
  直接发送JSON:
    json {"f":26}    直接发送原始MQTT命令
{'='*55}
""")

def main():
    global client
    
    print(f"[*] LAMBOT 扫地机 MQTT 控制客户端")
    print(f"[*] Broker: {BROKER}:{PORT}")
    print(f"[*] 设备UUID: {DEVICE_UUID}")
    print(f"[*] 正在连接...")
    
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
    
    # 等待连接
    for _ in range(10):
        if connected:
            break
        time.sleep(0.5)
    
    if not connected:
        print("[✗] 连接超时")
        return
    
    # 请求初始状态
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
        
        if cmd == 'quit' or cmd == 'exit':
            break
        elif cmd == 'help':
            print_help()
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
        elif cmd == 'forward':
            send_command('FORWARD')
        elif cmd == 'backward':
            send_command('BACKWARD')
        elif cmd == 'left':
            send_command('TURN_LEFT')
        elif cmd == 'right':
            send_command('TURN_RIGHT')
        elif cmd == 'status':
            # 先请求更新
            send_command('START_MQTT_UPDATE')
            time.sleep(2)
            print_status()
        elif cmd == 'battery':
            send_command('START_MQTT_UPDATE')
            time.sleep(1)
        elif cmd == 'map':
            send_command('ENTIRE_MAP')
        elif cmd == 'sensors':
            send_command('CHECK_SENSORS_STATUS')
        elif cmd == 'network':
            send_command('NETWORK_INFO')
        elif cmd == 'firmware':
            send_command('FIRMWARE_INFO')
        elif cmd == 'fan':
            send_command('GET_SWEEP_FAN_MODE')
        elif cmd == 'fan_query':
            send_command('GET_SWEEP_FAN_MODE')
        elif cmd == 'fan_normal':
            send_command('SET_SWEEP_FAN_MODE', 0)
        elif cmd == 'fan_silent':
            send_command('SET_SWEEP_FAN_MODE', 1)
        elif cmd == 'fan_high':
            send_command('SET_SWEEP_FAN_MODE', 2)
        elif cmd == 'fan_max':
            send_command('SET_SWEEP_FAN_MODE', 3)
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
        elif cmd == 'regions':
            send_command('GET_SWEEP_REGION')
        elif cmd.startswith('json '):
            try:
                json_str = cmd[5:]
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
