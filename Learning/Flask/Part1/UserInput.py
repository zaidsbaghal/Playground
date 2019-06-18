from flask import Flask
from flask import request
app = Flask(__name__)

@app.route('/Uppercase')
def uppercase():
    word = request.args.get('word')
    return word.upper()