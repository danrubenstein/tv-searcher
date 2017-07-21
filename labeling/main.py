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


@app.route('/')
def form():
    entries = load_set_to_label()
    return render_template('form_submit.html', entries=entries)


@app.route('/hello/', methods=['POST'])
def hello():

    form_as_dict = request.form.to_dict()
    
    keep_labeling = (form_as_dict.pop('Continue?', None) == 't')

    print(form_as_dict.keys())
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
    args = [(tuple([int(x) for x in list(labels_df['id'])]),)]
    PG_CONNECTION.execute(query, args)

    if keep_labeling:
        return redirect('/')
    else:
        return redirect('/thanks/')

@app.route('/thanks/')
def thanks(): 
    return render_template('thanks.html')


if __name__ == '__main__':

    app.run( 
        host="0.0.0.0",
        port=int("8080")
    )
    