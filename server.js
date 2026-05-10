require('dotenv').config();
const express = require('express');
const axios = require('axios');
const crypto = require('crypto');
const cors = require('cors');
const sqlite3 = require('sqlite3').verbose();
const mqtt = require('mqtt');
const { promisify } = require('util');

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));

// ==================== SQLite 数据库 ====================
const db = new sqlite3.Database('./data.db');
db.exec('PRAGMA journal_mode=WAL;');

const dbRun = promisify(db.run.bind(db));
const dbGet = promisify(db.get.bind(db));
const dbAll = promisify(db.all.bind(db));

// 数据库初始化标志
let dbInitialized = false;

// 创建表
(async () => {
    try {
        // 用户绑定表
        await dbRun(`
            CREATE TABLE IF NOT EXISTS user_bindings (
                openid TEXT PRIMARY KEY,
                uid TEXT NOT NULL,
                created_at INTEGER DEFAULT (strftime('%s', 'now'))
            )
        `);
        
        // 会话表
        await dbRun(`
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                openid TEXT NOT NULL,
                expire_at INTEGER NOT NULL
            )
        `);
        
        // 设备消息表
        await dbRun(`
            CREATE TABLE IF NOT EXISTS device_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uid TEXT NOT NULL,
                topic TEXT NOT NULL,
                msg TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                is_read INTEGER DEFAULT 0
            )
        `);
        
        // 创建索引
        await dbRun('CREATE INDEX IF NOT EXISTS idx_messages_uid_topic ON device_messages(uid, topic)');
        await dbRun('CREATE INDEX IF NOT EXISTS idx_messages_created ON device_messages(created_at)');
        
        dbInitialized = true;
        console.log('✅ 数据库初始化完成');
    } catch (err) {
        console.error('❌ 数据库初始化失败:', err);
        process.exit(1);
    }
})();

// ==================== 配置 ====================
const WX_APPID = process.env.WX_APPID;
const WX_SECRET = process.env.WX_SECRET;

if (!WX_APPID || !WX_SECRET) {
    console.error('❌ 请在.env文件中配置WX_APPID和WX_SECRET');
    process.exit(1);
}

function generateToken() {
    return crypto.randomBytes(32).toString('hex');
}

// ==================== MQTT 订阅巴法云 ====================
let bafaMqttClient = null;
let currentSubscribedUid = null;
let reconnectTimer = null;

// 存储设备消息到数据库
async function storeDeviceMessage(uid, topic, msg) {
    try {
        await dbRun(
            `INSERT INTO device_messages (uid, topic, msg, created_at, is_read) 
             VALUES (?, ?, ?, ?, 0)`,
            [uid, topic, msg, Date.now()]
        );
        console.log(`💾 [存储] ${topic} = ${msg}`);
    } catch (err) {
        console.error('存储失败:', err);
    }
}

// 订阅巴法云 MQTT
function subscribeBafaMqtt(uid) {
    if (!uid) {
        console.error('❌ UID 无效');
        return;
    }
    
    if (currentSubscribedUid === uid && bafaMqttClient && bafaMqttClient.connected) {
        console.log(`MQTT 已订阅 UID: ${uid}，跳过`);
        return;
    }
    
    if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
    }
    
    if (bafaMqttClient) {
        try {
            bafaMqttClient.end(true);
        } catch(e) {
            console.error('关闭旧连接错误:', e);
        }
        bafaMqttClient = null;
    }
    
    console.log(`🔌 开始订阅巴法云 MQTT，UID: ${uid}`);
    
    // 连接巴法云
    bafaMqttClient = mqtt.connect(`mqtt://bemfa.com:9501`, {
        clientId: uid,
        username: uid,
        password: '',
        keepalive: 60,
        clean: true,
        connectTimeout: 10000,
        reconnectPeriod: 5000
    });
    
    bafaMqttClient.on('connect', () => {
        console.log(`✅ MQTT 连接成功，已订阅 UID: ${uid}`);
        currentSubscribedUid = uid;
        
        // 订阅 door002 主题
        bafaMqttClient.subscribe('door002', { qos: 1 }, (err) => {
            if (err) {
                console.error('订阅失败:', err);
            } else {
                console.log('📡 已订阅主题: door002');
            }
        });
        
        // 也可以订阅通配符
        bafaMqttClient.subscribe('+', { qos: 1 }, (err) => {
            if (!err) {
                console.log('📡 已订阅所有主题');
            }
        });
    });
    
    bafaMqttClient.on('message', (topic, message) => {
        const msg = message.toString().trim();
        console.log(`📨 [MQTT消息] 主题: ${topic}, 内容: ${msg}`);
        
        if (currentSubscribedUid) {
            storeDeviceMessage(currentSubscribedUid, topic, msg);
        }
    });
    
    bafaMqttClient.on('error', (err) => {
        console.error('MQTT 错误:', err.message);
    });
    
    bafaMqttClient.on('close', () => {
        console.warn('MQTT 连接关闭');
        currentSubscribedUid = null;
    });
    
    bafaMqttClient.on('offline', () => {
        console.warn('MQTT 离线');
        currentSubscribedUid = null;
    });
    
    bafaMqttClient.on('reconnect', () => {
        console.log('MQTT 正在重连...');
    });
}

// 断开 MQTT 连接
function disconnectMqtt() {
    if (bafaMqttClient) {
        try {
            bafaMqttClient.end(true);
            console.log('🔌 MQTT 已断开');
        } catch(e) {
            console.error('断开 MQTT 错误:', e);
        }
        bafaMqttClient = null;
        currentSubscribedUid = null;
    }
}

// ==================== 发送 MQTT 指令到巴法云 ====================
async function sendMqttCommand(uid, topic, message) {
    try {
        const response = await axios.post('https://api.bemfa.com/api/device/v1/data/1/', {
            uid: uid,
            topic: topic,
            msg: message || ''
        }, {
            timeout: 5000,
            headers: { 'Content-Type': 'application/json' }
        });
        return { success: true, data: response.data };
    } catch (error) {
        console.error('HTTP发送失败:', error.message);
        return { success: false, error: error.message };
    }
}

// ==================== 1. 登录接口 ====================
app.post('/api/login', async (req, res) => {
    const { code } = req.body;
    
    if (!code) {
        return res.json({ code: -1, message: '缺少code参数' });
    }
    
    try {
        const wxUrl = `https://api.weixin.qq.com/sns/jscode2session?appid=${WX_APPID}&secret=${WX_SECRET}&js_code=${code}&grant_type=authorization_code`;
        const wxRes = await axios.get(wxUrl);
        const { openid, errcode, errmsg } = wxRes.data;
        
        if (errcode) {
            console.error('微信登录失败:', errcode, errmsg);
            return res.json({ code: -1, message: '微信登录失败: ' + (errmsg || '未知错误') });
        }
        
        console.log('用户登录:', openid);
        
        const binding = await dbGet('SELECT uid FROM user_bindings WHERE openid = ?', [openid]);
        
        if (!binding) {
            const tempToken = generateToken();
            const expireAt = Date.now() + 10 * 60 * 1000;
            
            await dbRun(
                'INSERT OR REPLACE INTO sessions (token, openid, expire_at) VALUES (?, ?, ?)',
                [tempToken, openid, expireAt]
            );
            
            return res.json({
                code: 1,
                message: '请绑定您的巴法云UID',
                tempToken: tempToken
            });
        }
        
        const token = generateToken();
        const expireAt = Date.now() + 7 * 24 * 60 * 60 * 1000;
        
        await dbRun(
            'INSERT OR REPLACE INTO sessions (token, openid, expire_at) VALUES (?, ?, ?)',
            [token, openid, expireAt]
        );
        
        // 登录成功后，启动 MQTT 订阅
        subscribeBafaMqtt(binding.uid);
        
        res.json({
            code: 0,
            message: '登录成功',
            token: token
        });
        
    } catch (error) {
        console.error('登录错误:', error.message);
        res.json({ code: -1, message: '服务器错误' });
    }
});

// ==================== 2. 绑定巴法云UID ====================
app.post('/api/bind-uid', async (req, res) => {
    const { tempToken, uid } = req.body;
    
    if (!tempToken || !uid) {
        return res.json({ code: -1, message: '参数错误' });
    }
    
    try {
        const session = await dbGet(
            'SELECT openid FROM sessions WHERE token = ? AND expire_at > ?',
            [tempToken, Date.now()]
        );
        
        if (!session) {
            return res.json({ code: -1, message: '绑定链接已过期' });
        }
        
        const openid = session.openid;
        
        // 保存绑定
        await dbRun(
            'INSERT OR REPLACE INTO user_bindings (openid, uid) VALUES (?, ?)',
            [openid, uid]
        );
        
        // 删除临时token
        await dbRun('DELETE FROM sessions WHERE token = ?', [tempToken]);
        
        // 生成正式token
        const token = generateToken();
        const expireAt = Date.now() + 7 * 24 * 60 * 60 * 1000;
        
        await dbRun(
            'INSERT OR REPLACE INTO sessions (token, openid, expire_at) VALUES (?, ?, ?)',
            [token, openid, expireAt]
        );
        
        // 绑定成功后，立即启动 MQTT 订阅
        subscribeBafaMqtt(uid);
        
        console.log(`✅ 绑定成功: ${openid} -> ${uid}`);
        
        res.json({
            code: 0,
            message: '绑定成功',
            token: token
        });
        
    } catch (error) {
        console.error('绑定错误:', error);
        res.json({ code: -1, message: '绑定失败' });
    }
});

// ==================== 3. 认证中间件 ====================
async function authMiddleware(req, res, next) {
    let token = req.headers.authorization;
    
    if (!token) {
        return res.json({ code: 401, message: '未登录' });
    }
    
    // 去除 Bearer 前缀（如果有）
    if (token.startsWith('Bearer ')) {
        token = token.slice(7);
    }
    
    try {
        const session = await dbGet(
            'SELECT openid FROM sessions WHERE token = ? AND expire_at > ?',
            [token, Date.now()]
        );
        
        if (!session) {
            return res.json({ code: 401, message: 'token无效或已过期' });
        }
        
        const binding = await dbGet(
            'SELECT uid FROM user_bindings WHERE openid = ?',
            [session.openid]
        );
        
        if (!binding) {
            return res.json({ code: 401, message: '请先完成UID绑定' });
        }
        
        req.session = {
            openid: session.openid,
            uid: binding.uid
        };
        next();
    } catch (err) {
        console.error('认证中间件错误:', err);
        res.json({ code: 401, message: '认证失败' });
    }
}

// ==================== 4. 发送MQTT指令 ====================
app.post('/api/send', authMiddleware, async (req, res) => {
    const { topic, message } = req.body;
    const uid = req.session.uid;
    
    if (!topic) {
        return res.json({ code: -1, message: '请提供topic' });
    }
    
    try {
        const result = await sendMqttCommand(uid, topic, message || '');
        
        if (result.success) {
            console.log(`📤 发送指令: ${uid}/${topic} -> ${message}`);
            res.json({ code: 0, message: '发送成功', data: result.data });
        } else {
            res.json({ code: -1, message: '发送失败: ' + result.error });
        }
    } catch (error) {
        console.error('发送错误:', error);
        res.json({ code: -1, message: '发送失败' });
    }
});

// ==================== 5. 查询设备消息 ====================
app.post('/api/messages', authMiddleware, async (req, res) => {
    const { topic, limit = 20, fromId } = req.body;
    const uid = req.session.uid;
    
    if (!topic) {
        return res.json({ code: -1, message: '请提供topic' });
    }
    
    try {
        let sql = `SELECT id, msg, created_at as timestamp 
                   FROM device_messages 
                   WHERE uid = ? AND topic = ?`;
        let params = [uid, topic];
        
        if (fromId && fromId > 0) {
            sql += ` AND id > ?`;
            params.push(parseInt(fromId));
        }
        
        sql += ` ORDER BY id ASC LIMIT ?`;
        params.push(Math.min(parseInt(limit), 100)); // 最多100条
        
        const messages = await dbAll(sql, params);
        
        res.json({
            code: 0,
            data: messages
        });
        
    } catch (error) {
        console.error('查询失败:', error);
        res.json({ code: -1, message: '查询失败' });
    }
});

// ==================== 6. 查询设备最新状态 ====================
app.post('/api/query', authMiddleware, async (req, res) => {
    const { topic } = req.body;
    const uid = req.session.uid;
    
    if (!topic) {
        return res.json({ code: -1, message: '请提供topic' });
    }
    
    try {
        const latest = await dbGet(
            `SELECT msg, created_at as timestamp 
             FROM device_messages 
             WHERE uid = ? AND topic = ? 
             ORDER BY id DESC LIMIT 1`,
            [uid, topic]
        );
        
        res.json({
            code: 0,
            data: latest || { msg: '', timestamp: 0 }
        });
        
    } catch (error) {
        console.error('查询状态失败:', error);
        res.json({ code: -1, message: '查询失败' });
    }
});

// ==================== 7. 查询所有设备状态 ====================
app.post('/api/all-status', authMiddleware, async (req, res) => {
    const uid = req.session.uid;
    
    try {
        const messages = await dbAll(
            `SELECT topic, msg, MAX(created_at) as timestamp, id
             FROM device_messages 
             WHERE uid = ?
             GROUP BY topic
             ORDER BY timestamp DESC`,
            [uid]
        );
        
        const status = {};
        for (const msg of messages) {
            status[msg.topic] = {
                msg: msg.msg,
                timestamp: msg.timestamp
            };
        }
        
        res.json({
            code: 0,
            data: status
        });
        
    } catch (error) {
        console.error('查询所有状态失败:', error);
        res.json({ code: -1, message: '查询失败' });
    }
});

// ==================== 8. 解绑 ====================
app.post('/api/unbind', authMiddleware, async (req, res) => {
    const openid = req.session.openid;
    
    try {
        await dbRun('DELETE FROM user_bindings WHERE openid = ?', [openid]);
        await dbRun('DELETE FROM sessions WHERE openid = ?', [openid]);
        
        // 如果没有其他绑定的用户，断开 MQTT
        const otherBindings = await dbGet('SELECT openid FROM user_bindings LIMIT 1');
        if (!otherBindings) {
            disconnectMqtt();
        }
        
        res.json({ code: 0, message: '解绑成功' });
    } catch (error) {
        console.error('解绑失败:', error);
        res.json({ code: -1, message: '解绑失败' });
    }
});

// ==================== 9. 登出 ====================
app.post('/api/logout', authMiddleware, async (req, res) => {
    const token = req.headers.authorization;
    try {
        await dbRun('DELETE FROM sessions WHERE token = ?', [token]);
        res.json({ code: 0, message: '已登出' });
    } catch (error) {
        res.json({ code: -1, message: '登出失败' });
    }
});

// ==================== 10. 健康检查 ====================
app.get('/health', (req, res) => {
    res.json({ 
        status: 'ok', 
        timestamp: Date.now(),
        dbReady: dbInitialized,
        mqttConnected: bafaMqttClient && bafaMqttClient.connected,
        currentUid: currentSubscribedUid
    });
});

// ==================== 11. 获取用户信息 ====================
app.get('/api/user-info', authMiddleware, async (req, res) => {
    res.json({
        code: 0,
        data: {
            openid: req.session.openid,
            uid: req.session.uid
        }
    });
});

// ==================== 启动服务器 ====================
const PORT = process.env.PORT || 3000;

// 等待数据库初始化后再启动自动订阅
const startAutoSubscribe = async () => {
    // 等待数据库初始化完成
    let retries = 10;
    while (!dbInitialized && retries > 0) {
        await new Promise(resolve => setTimeout(resolve, 500));
        retries--;
    }
    
    if (!dbInitialized) {
        console.log('⚠️ 数据库未完全初始化，跳过自动订阅');
        return;
    }
    
    try {
        const bindings = await dbAll('SELECT uid FROM user_bindings LIMIT 1');
        if (bindings && bindings.length > 0) {
            console.log(`📡 发现已绑定用户，自动订阅 UID: ${bindings[0].uid}`);
            subscribeBafaMqtt(bindings[0].uid);
        } else {
            console.log('ℹ️ 暂无绑定用户，等待首次绑定');
        }
    } catch (err) {
        console.log('ℹ️ 暂无绑定用户，等待首次绑定');
    }
};

app.listen(PORT, '0.0.0.0', () => {
    console.log('='.repeat(50));
    console.log(`✅ 后端服务已启动: http://0.0.0.0:${PORT}`);
    console.log(`📋 接口列表:`);
    console.log(`   POST /api/login         - 微信登录`);
    console.log(`   POST /api/bind-uid      - 绑定UID`);
    console.log(`   POST /api/send          - 发送指令`);
    console.log(`   POST /api/messages      - 查询消息（轮询）`);
    console.log(`   POST /api/query         - 查询单个状态`);
    console.log(`   POST /api/all-status    - 查询所有状态`);
    console.log(`   POST /api/unbind        - 解绑`);
    console.log(`   POST /api/logout        - 登出`);
    console.log(`   GET  /api/user-info     - 用户信息`);
    console.log(`   GET  /health            - 健康检查`);
    console.log('='.repeat(50));
    
    // 延迟启动自动订阅
    setTimeout(startAutoSubscribe, 2000);
});

// 优雅关闭
process.on('SIGINT', async () => {
    console.log('\n收到关闭信号，正在清理...');
    disconnectMqtt();
    await new Promise(resolve => db.close(resolve));
    console.log('数据库已关闭');
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('\n收到终止信号，正在清理...');
    disconnectMqtt();
    await new Promise(resolve => db.close(resolve));
    console.log('数据库已关闭');
    process.exit(0);
});
