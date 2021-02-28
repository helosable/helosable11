from flask import Flask, render_template
import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../log_analyzer")
from models.parser_data_manager import Parser_data_manager as pdm


with pdm('main.db') as dm:
    rep = dm.report()

headings = rep[0]
data = rep[1:]

app = Flask(__name__)

@app.route('/')
def table():
    return render_template('index.html', heading = headings, data = data)


if __name__ == "__main__":
    app.run(host = '127.0.0.1', port = '8080', debug = False)