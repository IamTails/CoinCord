import json
import uuid

import flask
import requests
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

  if request['type'] == "withdrawl" and list(r.table('users').filter(r.row['_id'] == request['user']['_id']))[0]['balance'] < request['amount']:
    return error_msg(request, "The specified user does not have the required funds.")

  request.update({'id': uuid.uuid1().int >> 64})

  r.table('transactions').insert({"transaction": dict(
      make_response(request, jsonify=False))}).run(conn)

  fields = []
  fields.append(
      {"name": "User", "value": f"{request['user']['name']}#{request['user']['discrim']} ({request['user']['_id']})"})
  fields.append(
      {"name": "Bot", "value": f"{request['bot']['name']}#{request['bot']['discrim'] ({request['bot']['_id']})}"})
  fields.append({"name": "Type", "value": request['type']})
  fields.append({"name": "Amount", "value": request['amount']})
  fields.append({"name": "Reason", "value": request['reason']})
  embed = {"title": "New transaction", "fields": fields}
  requests.post('https://canary.discordapp.com/api/webhooks/338371642277494786/vG8DJjpXC-NEXB4ZISo1r7QQ0Ras_RaqZbuhzjYOklKu70l73PmumdUCgBruypPv3fQp',
                json={"embeds": [embed]})

  return make_response(request)


@app.route('/api/admin/new_token', methods=['POST'])
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


@app.route('/api/admin/new_admin_token')
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


@app.route('/api/admin/transactions')
def show_transactions():
  raw_request = flask.request
  request = flask.request.get_json()
  if not raw_request.headers['Authorization'].split()[1] in list(r.table('tokens').run(conn)):
    return error_msg(request, "Invalid token")
  return flask.jsonify(list(r.table('transactions').run()))

@app.route('/api/admin/fake_transaction', methods=['POST'])
def fake_transaction():
  raw_request = flask.request
  request = flask.request.get_json()
  if not raw_request.headers['Authorization'].split()[1] in [token_list['token'] for token_list in list(r.table('tokens').run(conn))]:
    print(list(r.table('tokens').run(conn)))
    print(raw_request.headers['Authorization'].split()[1])
    return error_msg(request, "Invalid token")
  fields = []
  fields.append(
      {"name": "User", "value": f"{request['user']['name']}#{request['user']['discrim']} ({request['user']['_id']})"})
  fields.append(
      {"name": "Bot", "value": f"{request['bot']['name']}#{request['bot']['discrim']} ({request['bot']['_id']})"})
  fields.append({"name": "Type", "value": request['type']})
  fields.append({"name": "Amount", "value": request['amount']})
  fields.append({"name": "Reason", "value": request['reason']})
  fields.append({"name":"ID", "value":request['_id']})
  embed = {"title": "New transaction", "fields": fields}
  return requests.post('https://canary.discordapp.com/api/webhooks/338371642277494786/vG8DJjpXC-NEXB4ZISo1r7QQ0Ras_RaqZbuhzjYOklKu70l73PmumdUCgBruypPv3fQp',
                json={"embeds": [embed]})

if __name__ == '__main__':
  setup_db()
  app.run(debug=True, host='0.0.0.0', port=3000)
