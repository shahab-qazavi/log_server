from handlers.handlers import Collect, Logs, Login,Register, CustomLog, LastDashboard, Dashboard

url_patterns = [
    ("/v1/collect", Collect, None, "collect_v1"),
    ("/v1/register", Register, None, "register_v1"),
    ("/v1/login", Login, None, "login_v1"),
    ("/v1/logs/?([^/]+)?", Logs, None, "logs_v1"),
    ("/v1/customlog/?([^/]+)?", CustomLog, None, "customlogs_v1"),
    ("/v1/lastdashboard/?([^/]+)?", LastDashboard, None, "updatedashboard_v1"),
    ("/v1/dashboard/?([^/]+)?", Dashboard, None, "dashboard_v1"),
]
