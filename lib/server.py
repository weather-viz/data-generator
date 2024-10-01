from flask import Flask
from flask_cors import CORS
from lib.transform import Transformer
from lib.sql import Database
from lib.utils import split_by_days, split_by_weeks, split_by_months

app = Flask(__name__)
CORS(app)

@app.route("/weather_data/<station>/<sample_duration>/<observation>.json")
def get_json_data(station, sample_duration, observation):
    result = {
        'values': {}
    }
    for transformer in Transformer._transformers:
        query = transformer(station, observation, sample_duration)
        db = app.config['db']
        data = db.run_query(query)
        if sample_duration == '1d':
            data, x_axis, y_axis = split_by_days(data)
        elif sample_duration == '1w':
            data, x_axis, y_axis = split_by_weeks(data)
        elif sample_duration == '1m':
            data, x_axis, y_axis = split_by_months(data)
        result['values'][transformer.__name__] = data
        result['x_axis'] = x_axis
        result['y_axis'] = y_axis

    return result

@app.route("/index.json")
def get_stations():
    db = app.config['db']
    query = "SHOW TABLES"
    stations = db.run_query(query)
    observations = [c for (c, _) in Database.columns]
    observations.remove(Database.timestamp_column_name)
    return {
        'stations': [s[0].split('_', 1)[1] for s in stations],
        'observations': observations,
        'transformers': [t.__name__ for t in Transformer._transformers]
    }

def start_server(db: Database, host='0.0.0.0', port=5000, debug=True):
    """
    Start the Flask server.

    :param host: The host to run the server on. Defaults to '0.0.0.0'.
    :param port: The port to run the server on. Defaults to 5000.
    :param debug: Whether to run the server in debug mode. Defaults to False.
    """
    app.config['db'] = db
    app.run(host=host, port=port, debug=debug)