import ast
from flask import Flask, jsonify, request, Response, render_template

app = Flask(__name__)
data = {'labels': [], 'counts': []}

@app.route('/')
def index():
    return render_template("index.html", data=data)

@app.route('/refresh_sentiment')
def refresh_trending():
    global data

    return jsonify(
        Label=data['labels'],
        Count=data['counts'])


@app.route('/update', methods=['POST'])
def update_trending():
    global data
    if not request.form not in request.form:
        return "error", 400

    data['labels'] = ast.literal_eval(request.form['label'])
    data['counts'] = ast.literal_eval(request.form['count'])

    return "success", 201


if __name__ == "__main__":
    app.run(debug=True)
