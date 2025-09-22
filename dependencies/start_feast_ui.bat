@echo off
chcp 65001 > nul
echo 启动Feast Web UI...

echo.
echo 第1步：检查Feast Pod状态...
for /f "tokens=1" %%i in ('kubectl get pods -l app.kubernetes.io/name=feast -o jsonpath="{.items[0].metadata.name}" 2^>nul') do set FEAST_POD=%%i

if "%FEAST_POD%"=="" (
    echo [错误] 找不到Feast Pod，请确保Feast已正确部署
    pause
    exit /b 1
)

echo [成功] 找到Feast Pod: %FEAST_POD%

echo.
echo 第2步：启动Feast Web UI进程...
kubectl exec %FEAST_POD% -c online -- bash -c "nohup feast ui --host 0.0.0.0 --port 8080 > /tmp/ui.log 2>&1 &"
if %errorlevel% neq 0 (
    echo [错误] 启动Feast UI失败
    pause
    exit /b 1
)

echo [成功] Feast UI进程已启动

echo.
echo 第3步：等待UI进程完全启动...
timeout /t 3 /nobreak > nul

echo.
echo 第4步：验证UI进程状态...
kubectl exec %FEAST_POD% -c online -- ps aux | findstr "feast ui"
if %errorlevel% neq 0 (
    echo [警告] 未找到UI进程，可能启动失败
) else (
    echo [成功] UI进程运行正常
)

echo.
echo 第5步：设置端口转发 (localhost:8889 -> Pod:8080)...
echo [提示] 端口转发将在后台运行，按Ctrl+C停止
echo [访问] 请打开浏览器访问: http://localhost:8889
echo.

start /b kubectl port-forward %FEAST_POD% 8889:8080

echo Feast Web UI已就绪！
echo 访问地址: http://localhost:8889
echo.
pause
