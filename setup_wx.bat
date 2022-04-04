pause
call C:\Miniconda3\Scripts\activate.bat
call conda activate wx1
python setup_package.py bdist_wheel --dist-dir=S:\WX\Models\PROD\wx_wheels
pause
@RD /S /Q "build"
@RD /S /Q "Wx.egg-info"
echo "Build successful!"
echo "Deleted build and Wx.egg-info folders post build."
pause
