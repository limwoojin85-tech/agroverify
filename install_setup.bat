@echo off
chcp 949 > nul
echo ==========================================
echo   agroverify 설치
echo   - innong 와 독립된 검수 전용 GUI
echo   - agromarket.kr vs C:\agro_data_v2\daily 비교
echo ==========================================
echo.

python --version > nul 2>&1
if %errorlevel% neq 0 (
    echo [오류] Python 미설치. https://python.org 에서 3.11+ 설치 후 PATH 등록.
    pause
    exit /b 1
)

echo [확인] Python OK
python --version
echo.

echo pip 업그레이드 중...
python -m pip install --upgrade pip --quiet
echo.

echo 패키지 설치 중 (requests 만 필요 - 표준 라이브러리 + Tkinter 외)
python -m pip install requests urllib3
echo.

echo ==========================================
echo  설치 완료
echo  실행: python run.py
echo ==========================================
pause
