目录结构
.
├── ChangeLog
├── demo
│   ├── push_server.py
│   └── requirements
├── etc
│   ├── pusher_config.py
│   └── supervisor.conf
├── lib
│   ├── dbpc.py
│   ├── mwconfig.py
│   ├── mwlogger.py
│   ├── pusher_utils.py
│   └── rdao.py
├── mkpkg.sh
├── mw_pusher
│   └── mwPusher.py
├── mw_repusher
│   └── mwRepusher.py
├── PROG_VERSION.def
├── readme.txt
├── requirements
├── setup.py
├── test
└── tools
    └── config_update.py



mw_pusher: 推送模块主程序
mw_repusher: 重试推送模块主程序
etc: 配置文件
lib: 自实现的库
requirements: 第三方依赖
demo: 实现了接收数据的服务
ChangeLog: 记录了版本变更内容，最顶上为最新版本
tools: 工具集合目录
setup.py: 安装脚本
test: 测试代码目录

安装:
1. 先安装requirements依赖
 * sudo pip install -r requirements
2. python setup.py <mw_pusher|mw_repusher|all> -d install_dir [--autostart] [--upgrade|--rollback]
 * setup.py会按照etc/supervisor.conf的配置启动supervisord

启动与停止
1. 用supervisorctl控制启动和停止
 supervisorctl -c ./etc/supervisor.conf reload  # 重启 supervisord 进程, 此步骤不是必须的，除非改动了etc/supervisor.conf文件
 supervisorctl -c ./etc/supervisor.conf start|stop|restart mw_pusher|mw_repusher|all

升级与回滚
 * N/A

