import json
import uuid

import flask
import rethinkdb as r
from oauth2 import tokengenerator

app = flask.Flask(__name__)


class Database:
  def __init__(self, host='localhost', port=28015, db='test'):
    self.host, self.port = host, port
    self.db = db

  def connect(self):
    return r.connect(host=self.host, port=self.port, db=self.db)


conn = Database(db='coincord').connect()


def setup_db():
  """
  creates db, tables, indexes if they don't exist
  """
  try:  # make 'coincord' db if it doesn't exist
    r.db('coincord').run(conn)
  except r.errors.ReqlOpFailedError:
    r.db_create('coincord').run(conn)
  except r.errors.ReqlQueryLogicError:
    pass
  try:  # make 'transactions' table if it doesn't exist
    r.table('transactions').run(conn)
  except r.errors.ReqlOpFailedError:
    r.db('coincord').table_create('transactions').run(conn)
  try:  # make 'meta' table if it doesn't exist
    r.table('tokens').run(conn)
  except:
    r.db('coincord').table_create('tokens').run(conn)
  try:  # make 'bots' table if it doesn't exist
    r.table('bots').run(conn)
  except:
    r.db('coincord').table_create('bots').run(conn)
  try:  # make 'users' table if it doesn't exist
    r.table('users').run(conn)
  except r.errors.ReqlOpFailedError:
    r.db('coincord').table_create('users').run(conn)
  try:  # make 'ta_id' index if it doesn't exist
    r.table('transactions').index_wait('_id').run(conn)
  except r.errors.ReqlOpFailedError:
    r.table('transactions').index_create('_id').run(conn)
  try:  # make 'user_id' index if it doesn't exist
    r.table('users').index_wait('_id').run(conn)
  except r.errors.ReqlOpFailedError:
    r.table('users').index_create('_id').run(conn)
  if len(list(r.table('tokens').run(conn))) == 0:
    token = tokengenerator.URandomTokenGenerator().generate()
    r.table('tokens').insert({"token": token}).run(conn)
    print(f"No admin tokens set, created new token {token}")


def error_msg(request, message, jsonify=True):
  try:
    output = {"status": "error", "error": message,
              "type": request['type'], "amount": request['amount'], "user": request['user']}
  except (KeyError, TypeError) as e:
    output = {"status": "error", "error": message}
  if jsonify:
    return flask.jsonify(output)
  return output


def make_response(request, jsonify=True):
  output = {"id": request['id'], "status": "success", "type": request['type'],
            "amount": request['amount'], "user": request['user']}
  if jsonify:
    return flask.jsonify(output)
  return output


@app.route('/api/new_transaction', methods=['POST'])
def new_transaction():
  raw_request = flask.request
  request = flask.request.get_json()
  try:
    if not raw_request.headers['Authorization'].split()[1] in [token_list['token'] for token_list in list(r.table('tokens').run(conn))]:
      if not raw_request.headers['Authorization'].split()[1] == list(r.table('bots').filter(r.row['bot']['_id'] == request['bot']['_id']).run(conn))[0]['token']:
        return error_msg(request, "Invalid token")
  except KeyError:
    return error_msg(request, "Invalid token")
  if not request:
    return error_msg(request, 'Request cannot be empty')
  if not "bot" and "user" and "server" and "type" and "third-party" and "amount" and "reason" in request or "" in request.values():
    return error_msg(request, "Invalid request format")
  if request['amount'] <= 0:
    return error_msg(request, "Amount must be greater than 0")
  request.update({'id': uuid.uuid1().int >> 64})

  if request['type'] == "withdrawl" and list(r.table('users').filter(r.row['_id'] == request['user']['_id']))[0]['balance'] < request['amount']:
    return error_msg(request, "The specified user does not have the required funds.")

  return make_response(request)


@app.route('/api/new_token', methods=['POST'])
def create_token():
  raw_request = flask.request
  request = flask.request.get_json()
  if not raw_request.headers['Authorization'].split()[1] in list(r.table('tokens').run(conn)):
    return error_msg(request, "Invalid token")
  token = tokengenerator.URandomTokenGenerator().generate()
  bot = {"name": request['bot']['name'], "discrim": request['bot']['discrim'],
         "_id": request['bot']['id'], "owner": request['owner'], "token": token}
  r.table('bots').insert({"bot": bot})
  return flask.jsonify(bot)


@app.route('/api/new_admin_token')
def create_admin_token():
  raw_request = flask.request
  request = flask.request.get_json()
  try:
    if not raw_request.headers['Authorization'].split()[1] in list(r.table('tokens').run(conn)):
      return error_msg(request, "Invalid token")
  except KeyError:
    return error_msg(request, "Invalid token")
  token = tokengenerator.URandomTokenGenerator().generate()
  r.table('tokens').insert({"token": token}).run(conn)
  return token


if __name__ == '__main__':
  setup_db()
  app.run(debug=True, host='0.0.0.0', port=3000)
