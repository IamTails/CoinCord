import json
import uuid

import flask
import requests
import rethinkdb as r
from datetime import datetime
from oauth2 import tokengenerator
from functools import wraps

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


  

def require_auth(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        raw_request = flask.request
        request = flask.request.get_json()
        try:
            if not raw_request.headers['Authorization'].split()[0] in [token_list['token'] for token_list in list(r.table('tokens').run(conn))]:
              return error_msg(request, "Invalid token")
        except KeyError:
            return error_msg(request, "Invalid token")

        return f(*args, **kwargs)
    return wrapper

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
    return flask.jsonify(output),401
  return output,401


def make_response(request, jsonify=True):
  """
  creates success response with the given request
  """
  output = {"_id": request['_id'], "status": "success", "type": request['type'],
            "amount": request['amount'], "user": request['user'], "bot": request['bot'], "reason": request['reason']}
  if jsonify:
    return flask.jsonify(output)
  return output


def make_embed(request, message="New Transaction"):
  """
  create an embed for logging transactions
  """
  fields = []
  fields.append({"name": "ID", "value": request['_id']})
  if request['type'] == "deposit":
    fields.append({"name": "Amount", "value": f"+{request['amount']}"})
  elif request['type'] == "withdrawl":
    fields.append({"name": "Amount", "value": f"-{request['amount']}"})
  fields.append({"name": "Reason", "value": request['reason']})
  fields.append({"name": "Balance", "value": request['user']['balance']})
  fields.append(
      {"name": "User", "value": f"{request['user']['name']}#{request['user']['discrim']} ({request['user']['_id']})"})
  fields.append(
      {"name": "Bot", "value": f"{request['bot']['name']}#{request['bot']['discrim']} ({request['bot']['_id']})"})
  embed = {"title": message, "fields": fields, "timestamp":datetime.now().isoformat()}
  test = requests.post('https://canary.discordapp.com/api/webhooks/338371642277494786/vG8DJjpXC-NEXB4ZISo1r7QQ0Ras_RaqZbuhzjYOklKu70l73PmumdUCgBruypPv3fQp', json={"embeds": [embed]})

def get_transactions(request):
  transactions = []
  if "type" in request:
    transactions.append(list(r.table('transactions').filter(
        r.row['type'] == request['type']).run(conn)))
  if "amount" in request:
    transactions.append(list(r.table('transactions').filter(
        r.row['amount'] == request['amount']).run(conn)))
  if "bot" in request:
    transactions.append(list(r.table('transactions').filter(
        r.row['bot'] == request['bot']).run(conn)))
  if "user" in request:
    transactions.append(list(r.table('transactions').filter(
        r.row['user'] == request['user']).run(conn)))
  if "reason" in request:
    transactions.append(list(r.table('transactions').filter(
        r.row['reason'] == request['reason']).run(conn)))
  if not "reason" or "user" or "bot" or "amount" or "type" in request:
    transactions.append(list(r.table('transactions').run(conn)))
  temp = []
  for i in transactions:
    if i not in temp:
      temp.append(i)
  transactions = temp
  return transactions[:request['limit']]


@app.route('/api/new_transaction', methods=['POST'])
@require_auth
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
  if not request:
    return error_msg(request, 'Request cannot be empty')
  if not "bot" and "user" and "server" and "type" and "third-party" and "amount" and "reason" in request or "" in request.values():
    return error_msg(request, "Invalid request format")
  if request['amount'] <= 0:
    return error_msg(request, "Amount must be greater than 0")

  if request['type'] == "withdrawl" and list(r.table('users').filter(r.row['_id'] == request['user']['_id']))[0]['balance'] < request['amount']:
    return error_msg(request, "The specified user does not have the required funds.")

  request.update({'_id': uuid.uuid1().int >> 64})

  r.table('transactions').insert({"transaction": dict(
      make_response(request, jsonify=False))}).run(conn)
  if len(list(r.table('users').filter(r.row['user']['_id'] == request['user']['_id']).run(conn))) == 0:
    request['user'].update({"balance": 0})
    r.table('users').insert(
        {"user": request['user']}).run(conn)
  balance = int(list(r.table('users').filter(r.row['user']['_id'] == request['user']['_id']).run(conn))[0]['user']['balance'])
  if request['type'] == "deposit":
    request['user']['balance'] = balance + request['amount']
  if request['type'] == "withdrawl":
    request['user']['balance'] = balance - request['amount']
  r.table('users').filter(r.row['user']['_id'] == request['user']['_id']).update(
      {"balance": request['user']['balance']}).run(conn)

  print("You should be maing an embed")
  make_embed(request)
  print("Why aren't you?")

  return make_response(request)


@app.route('/api/admin/new_token', methods=['POST'])
@require_auth
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
  
  token = tokengenerator.URandomTokenGenerator().generate()
  bot = {"name": request['bot']['name'], "discrim": request['bot']['discrim'],
         "_id": request['bot']['id'], "owner": request['owner'], "token": token}
  r.table('bots').insert({"bot": bot})
  return flask.jsonify(bot)


@app.route('/api/admin/new_admin_token', methods=['GET'])
@require_auth
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


@app.route('/api/admin/transactions', methods=["POST"])
@require_auth
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
  transactions = get_transactions(request)
  return flask.jsonify(transactions)


@app.route('/api/admin/fake_transaction', methods=['POST'])
@require_auth
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
  make_embed(request)
  return flask.jsonify(request)


@app.route('/api/admin/purge_transactions', methods=['POST'])
@require_auth
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
  all_transactions = list(r.table('transactions').filter(
      r.row['_id'] >= request['_id'] and r.row['bot']['_id'] == request['bot']['_id']).run(conn))
  if len(all_transactions) == 0:
    return error_msg(request, "No transactions found")
  for transaction in all_transactions:
    if transaction['type'] == "deposit":
      new_balance = int(f"-{transaction['amount']}")
    elif transaction['type'] == "withdrawl":
      new_balance = int(f"{transaction['amount']}")
    balance = int(list(r.table('users').filter(
        r.row['_id'] == transaction['user']['_id']).run(conn)))
    r.table('users').filter(r.row['_id'] == transaction['user']['_id']).update(
        {"balance": balance + new_balance})
    r.table('transactions').filter(
        r.row['_id'] == transaction['_id']).delete().run(conn)
    make_embed(transaction, "transaction reverted")
  return flask.jsonify(request)


@app.route('/api/admin/delete_transaction', methods=['POST'])
@require_auth
def delete_transaction():
  """
  Reverts a single given transaction

  {
    "_id": 90832
  }
  """
  raw_request = flask.request
  request = flask.request.get_json()
  transaction = list(r.table('transactions').filter(
      r.row['transaction']['_id'] == request['_id']).run(conn))[0]['transaction'] # get transaction from id
  if transaction['type'] == "deposit":
    new_balance = int(f"-{transaction['amount']}") # add or subtract to balance based on type
  elif transaction['type'] == "withdrawl":
    new_balance = int(f"{transaction['amount']}")
  balance = int(list(r.table('users').filter(
      r.row['user']['_id'] == transaction['user']['_id']).run(conn))[0]['user']['balance']) # get the user's current balance
  r.table('users').filter(r.row['user']['_id'] == transaction['user']['_id']).update(
      {"balance": balance + new_balance}) # change the user's balance
  r.table('transactions').filter(
      r.row['transaction']['_id'] == transaction['_id']).delete().run(conn) # delete the transaction
  transaction['user'].update({"balance": balance + new_balance})
  make_embed(transaction, "Transaction reverted")
  return flask.jsonify(transaction)

if __name__ == '__main__':
  setup_db()
  app.run(debug=True, host='0.0.0.0', port=3000)
