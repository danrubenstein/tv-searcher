import http.server
import socketserver
import datetime

import pandas as pd 
from flask import Flask, render_template, request, url_for, redirect

from utils import load_set_to_label, PG_ENGINE, PG_CONNECTION

PORT = 8000

# Initialize the Flask application
app = Flask(__name__)
app.tear_downfunctions = []

# Define a route for the default URL, which loads the form
@app.route('/')
def form():
    entries = load_set_to_label()
    return render_template('form_submit.html', entries=entries)

# Define a route for the action of the form, for example '/hello/'
# We are also defining which type of requests this route is 
# accepting: POST requests in this case
@app.route('/hello/', methods=['POST'])

def hello():

    form_as_dict = request.form.to_dict()

    relevant_examples = [int(key) for key in form_as_dict.keys() if form_as_dict[key] == 't']
    non_relevant_examples = [int(key) for key in form_as_dict.keys() if form_as_dict[key] == 'f']
    
    relevant_dicts = [{"id" : x, "label" : 1} for x in relevant_examples]
    non_relevant_dicts = [{"id" : x, "label" : 0} for x in non_relevant_examples]

    labels_df = pd.DataFrame(relevant_dicts+non_relevant_dicts)
    labels_df["input_time"] = str(datetime.datetime.now())
    labels_df.to_sql("tweets_labeled", PG_ENGINE, if_exists="append", schema="label_data", index=False)

    query = """
        DELETE from label_data.tweets_not_labeled
        WHERE id in %s
        """
    args = [(tuple(list(labels_df['id'])),)]
    PG_CONNECTION.execute(query, args)

    return redirect('/')

def x():
    print('ping!!')

app.tear_downfunctions.append(x)

if __name__ == '__main__':

    try:
        app.run( 
            host="0.0.0.0",
            port=int("8080")
        )
    except KeyboardInterrupt:
        print('ping')
        query = """
        UPDATE label_data.tweets_not_labeled
        SET label_in_process = False
        WHERE label_in_process = True
        """
        PG_CONNECTION.execute(query)