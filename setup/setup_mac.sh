# create virtual environment
python3 -m venv env

# active virtual environment
source env/bin/activate

# require latest pip
python3 -m pip install --upgrade pip

# install dependencies to virtual environment
python3 -m pip install --force-reinstall -r dependencies.pip

# set up python path
export PYTHONPATH="$(dirname $(pwd))/src:$PYTHONPATH"
