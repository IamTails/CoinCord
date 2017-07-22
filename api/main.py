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
  """
  creates an error message with the given request and message
  """
  try:
    output = {"status": "error", "error": message,
              "type": request['type'], "amount": request['amount'], "user": request['user']}
  except (KeyError, TypeError) as e:
    output = {"status": "error", "error": message}
  if jsonify:
    return flask.jsonify(output)
  return output


def make_response(request, jsonify=True):
  """
  creates success response with the given request
  """
  output = {"id": request['id'], "status": "success", "type": request['type'],
            "amount": request['amount'], "user": request['user']}
  if jsonify:
    return flask.jsonify(output)
  return output

def make_embed(request):
  """
  create an embed for logging transactions
  """
  fields = []
  fields.append({"name": "ID", "value": request['_id']})
  fields.append({"name": "Type", "value": request['type']})
  fields.append({"name": "Amount", "value": request['amount']})
  fields.append({"name": "Reason", "value": request['reason']})
  fields.append({"name": "Balance", "value": request['balance']})
  fields.append(
      {"name": "User", "value": f"{request['user']['name']}#{request['user']['discrim']} ({request['user']['_id']})"})
  fields.append(
      {"name": "Bot", "value": f"{request['bot']['name']}#{request['bot']['discrim'] ({request['bot']['_id']})}"})
  embed = {"title": "New transaction", "fields": fields}
  return embed

def get_transactions(request):
  transactions = []
  if "type" in request:
    transactions.append(list(r.table('transactions').filter(r.row['type'] == request['type']).run()))
  if "amount" in request:
    transactions.append(list(r.table('transactions').filter(r.row['amount'] == request['amount']).run()))
  if "bot" in request:
    transactions.append(list(r.table('transactions').filter(r.row['bot'] == request['bot']).run()))
  if "user" in request:
    transactions.append(list(r.table('transactions').filter(r.row['user'] == request['user']).run()))
  if "reason" in request:
    transactions.append(list(r.table('transactions').filter(r.row['reason'] == request['reason']).run()))
  transactions = list(set(transactions))
  return transactions[:request['limit']]


@app.route('/api/new_transaction', methods=['POST'])
def new_transaction():
  """
  main endpoint, used by bots to start transactions

  {
    "type": "deposit",
    "amount": 200,
    "user": {
      "name": "Mr Boo Grande",
      "discrim": "6644",
      "_id": "209137943661641728"
    },
    "bot": {
      "name": "Alpha Bot",
      "discrim": "4112",
      "_id": "331841835733614603"
    },
    "reason": "casino"
  }
  """
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

  if len(list(r.table('users').filter(r.row['_id'] == request['user']['_id']).run(conn))) == 0:
    r.table('users').insert({"user": request['user'].update({"balance": 0})}).run(conn)
  balance = int(list(r.table('users').filter(r.row['_id'] == request['user']['_id']).run(conn))[0])
  if request['type'] == "deposit":
    request['balance'] = balance + request['amount']
  if request['type'] == "withdrawl":
    request['balance'] = balance - request['amount']
  r.table('users').filter(r.row['_id'] == request['user']['_id']).update({"balance": request['balance']})

  requests.post('https://canary.discordapp.com/api/webhooks/338371642277494786/vG8DJjpXC-NEXB4ZISo1r7QQ0Ras_RaqZbuhzjYOklKu70l73PmumdUCgBruypPv3fQp',
                json={"embeds": [make_embed(request)]})

  return make_response(request)


@app.route('/api/admin/new_token', methods=['POST'])
def create_token():
  """
  register a new bot, and return the token

  "bot": {
    "name": "Alpha Bot",
    "discrim": "4112",
    "_id": "331841835733614603"
  }
  """
  raw_request = flask.request
  request = flask.request.get_json()
  if not raw_request.headers['Authorization'].split()[1] in [token_list['token'] for token_list in list(r.table('tokens').run(conn))]:
    return error_msg(request, "Invalid token")
  token = tokengenerator.URandomTokenGenerator().generate()
  bot = {"name": request['bot']['name'], "discrim": request['bot']['discrim'],
         "_id": request['bot']['id'], "owner": request['owner'], "token": token}
  r.table('bots').insert({"bot": bot})
  return flask.jsonify(bot)


@app.route('/api/admin/new_admin_token', methods=['GET'])
def create_admin_token():
  """
  create a new admin token (for /api/admin/ endpoints)
  """
  raw_request = flask.request
  request = flask.request.get_json()
  try:
    if not raw_request.headers['Authorization'].split()[1] in [token_list['token'] for token_list in list(r.table('tokens').run(conn))]:
      return error_msg(request, "Invalid token")
  except KeyError:
    return error_msg(request, "Invalid token")
  token = tokengenerator.URandomTokenGenerator().generate()
  r.table('tokens').insert({"token": token}).run(conn)
  return token


@app.route('/api/admin/transactions')
def show_transactions():
  """
  Returns all transactions that match the given parameters
  (keys in parentheses are optional)
  {
    "limit": 20,
    ("type": "deposit",)
    ("amount": 200,)
    ("user": {
      "name": "Mr Boo Grande",
      "discrim": "6644",
      "_id": "209137943661641728"
    },)
    ("bot": {
      "name": "Alpha Bot",
      "discrim": "4112",
      "_id": "331841835733614603"
    },)
    ("reason": "casino")
  }
  """
  raw_request = flask.request
  request = flask.request.get_json()
  if not raw_request.headers['Authorization'].split()[1] in [token_list['token'] for token_list in list(r.table('tokens').run(conn))]:
    return error_msg(request, "Invalid token")
  transactions = get_transactions(request)
  return flask.jsonify(transactions)

@app.route('/api/admin/fake_transaction', methods=['POST'])
def fake_transaction():
  """
  create a fake transaction log (for testing purposes)

  {
    "_id": 90832,
    "type": "deposit",
    "amount": 200,
    "user": {
      "name": "Mr Boo Grande",
      "discrim": "6644",
      "_id": "209137943661641728"
    },
    "bot": {
      "name": "Alpha Bot",
      "discrim": "4112",
      "_id": "331841835733614603"
    },
    "reason": "casino"
  }
  """
  raw_request = flask.request
  request = flask.request.get_json()
  if not raw_request.headers['Authorization'].split()[1] in [token_list['token'] for token_list in list(r.table('tokens').run(conn))]:
    return error_msg(request, "Invalid token")
  return requests.post('https://canary.discordapp.com/api/webhooks/338371642277494786/vG8DJjpXC-NEXB4ZISo1r7QQ0Ras_RaqZbuhzjYOklKu70l73PmumdUCgBruypPv3fQp',
                json={"embeds": [make_embed(request)]}).text

@app.route('/api/admin/purge_transactions', methods=['POST'])
def purge_transactions():
  """
  Revert all transactions from the given bot, upto (and including) the given transaction id

  {
    "bot": {
      "name": "Alpha Bot",
      "discrim": "4112",
      "_id": "331841835733614603"
    },
    "_id": 90832
  }
  """
  raw_request = flask.request
  request = flask.request.get_json()
  if not raw_request.headers['Authorization'].split()[1] in [token_list['token'] for token_list in list(r.table('tokens').run(conn))]:
    return error_msg(request, "Invalid token")
  all_transactions = list(r.table('transactions').filter(r.row['_id'] >= request['_id'] and r.row['bot']['_id'] == request['bot']['_id']).run())
  if len(all_transactions) == 0:
    return error_msg(request, "No transactions found")
  return error_msg(request, "Not implemented yet")

@app.route('/api/admin/delete_transaction', methods=['POST'])
def delete_transaction():
  """
  Reverts a single given transaction

  {
    "_id": 90832
  }
  """
  raw_request = flask.request
  request = flask.request.get_json()
  if not raw_request.headers['Authorization'].split()[1] in [token_list['token'] for token_list in list(r.table('tokens').run(conn))]:
    return error_msg(request, "Invalid token")
  return error_msg(request, "Not implemented yet")

if __name__ == '__main__':
  setup_db()
  app.run(debug=True, host='0.0.0.0', port=3000)
