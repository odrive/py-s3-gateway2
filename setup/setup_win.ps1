# create virtual environment
python -m venv env

# active virtual environment
./env/Scripts/Activate.ps1

# require latest pip
python -m pip install --upgrade pip

# install dependencies to virtual environment
python -m pip install --upgrade -r dependencies.pip

# set up python path
$STARTDIR=$((Get-Item -Path ".\" -Verbose).FullName)
if ($(Get-Item "$STARTDIR").basename -ne "setup" ){
    echo "Please start this script from the setup folder"
    return 1
}$BASEDIR="$((Get-Item $STARTDIR).parent.fullname)"
$PYTHONPATH=$(Split-Path (get-command python).path)
$env:PYTHONPATH=$PYTHONPATH + ";" + $env:PYTHONPATH
$env:PYTHONPATH = "${BASEDIR}\src;" + $env:PYTHONPATH
