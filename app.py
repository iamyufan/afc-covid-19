import sys
from flask import Flask, render_template
import json

# create web app's instance
app = Flask(__name__, static_url_path='', static_folder='build')
# CORS(app)

# Load json data
with open('data.json', 'r') as f:
    data = json.load(f)

# Get the last date in the data
last_date = list(data['1']['dates'].keys())[-1]

# Create a route for the home page
@app.route('/')
def home():
    return render_template("index.html")


# Get the whole data
@app.route('/data')
def get_data():
    return data

# Get the data where the posted center_id is located
@app.route('/center/<center_id>')
def get_center_data(center_id):
    # Iterate through each state in the data
    for state_id, state_data in data.items():
        # Iterate through each center in the state
        for c_id, c_data in state_data['centers'].items():
            # If the center_id matches, return the state data
            if c_id == center_id:
                return data[state_id]

    # If no center_id matches, return an empty dictionary
    return {}

# Get the data where the posted date is located
@app.route('/date/<date>', methods=['GET'])
def get_date_data(date):
    '''
    date format: YYYY-MM-DD
    '''
    if date > last_date or date < '2020-03-13':
        return "Invalid Date"

    # Iterate through each state in the data
    new_dict = {}
    for state_id, state_data in data.items():
        # Iterate through each center in the state
        for d_id, d_data in state_data['dates'].items():
            # If the date matches, return the state data
            if d_id == date:
                new_dict[state_id] = {
                    "state_name": state_data['state_name'],
                    "data": d_data
                }

    return new_dict

# Get the data where the posted state_id is located
@app.route('/state/<state_id>')
def get_state_data(state_id):
    # Iterate through each state in the data
    for s_id, s_data in data.items():
        # If the state_id matches, return the state data
        if s_id == state_id:
            return data[state_id]

    # If no state_id matches, return an empty dictionary
    return {}

if __name__ == "__main__":
    app.run()
