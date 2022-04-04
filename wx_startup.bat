ECHO pyxll-set-progress 0
ECHO pyxll-set-progress-status "StartingUpdate"
ECHO pyxll-show-progress
cd /D S:\WX\Models\PROD\wx_wheels
call C:\Miniconda3\Scripts\activate.bat
call conda activate wx1
REM call conda install -y --file conda_reqs.txt
REM call pip install -r requirements.txt
ECHO pyxll-set-progress 50
ECHO pyxll-set-progress-status "UpdatingWx"
call python wx_updater.py
ECHO pyxll-set-progress 90
ECHO pyxll-set-progress-status "ImportingModules"
ECHO pyxll-set-option PYXLL modules wx.xlapi.vanilla