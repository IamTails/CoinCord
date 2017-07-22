import flask
import json
import rethinkdb as r
import uuid

r.connect('localhost', 28015).repl()
app = flask.Flask(__name__)

def error_msg(request, message, jsonify=True):
  try:
    output = {"status":"error","error":message,"type":request['type'],"amount":request['amount'],"user":request['user']}
  except (KeyError, TypeError) as e:
    output = {"status":"error","error":message}
  if jsonify:
    return flask.jsonify(output)
  return output

def make_response(request, jsonify=True):
  output = {"id":request['id'],"status":"success","type":request['type'],"amount":request['amount'],"user":request['user']}
  if jsonify:
    return flask.jsonify(output)
  return output

@app.route('/api/new_transaction', methods=['POST'])
def new_transaction():
  request = flask.request.get_json()
  if not request: return(error_msg(request, 'Request cannot be empty'))
  if not "bot" and "user" and "server" and "type" and "third-party" and "amount" and "reason" in request or "" in request.values(): return(error_msg(request, "Invalid request format"))
  if request['amount'] <= 0: return(error_msg(request, "Amount must be greater than 0"))
  request.update({'id': uuid.uuid1().int>>64})
  return make_response(request)

if __name__ == '__main__':
  app.run(debug=True, host='0.0.0.0', port=8080)
