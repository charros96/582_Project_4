from flask import Flask, request, g
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from flask import jsonify
import json
import eth_account
import algosdk
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import load_only
from datetime import datetime
import sys

from models import Base, Order, Log
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)

app = Flask(__name__)

@app.before_request
def create_session():
    g.session = scoped_session(DBSession)

@app.teardown_appcontext
def shutdown_session(response_or_exc):
    sys.stdout.flush()
    g.session.commit()
    g.session.remove()


""" Suggested helper methods """

def check_sig(payload,sig):
    
    platform = payload.get('platform')

    pk = payload.get('sender_pk')
    result = False
    if platform == "Ethereum":
        eth_encoded_msg = eth_account.messages.encode_defunct(text =json.dumps(payload))
        
        if eth_account.Account.recover_message(eth_encoded_msg,signature=sig) == pk:
                result = True
    elif platform == "Algorand":
        algo_encoded_msg = json.dumps(payload).encode('utf-8')
        if algosdk.util.verify_bytes(algo_encoded_msg,sig,pk):
            result = True
    return result
    
def process_order(content):
    order = content.get('payload')
    signature = content.get('sig')
    
    fields = ['sender_pk','receiver_pk','buy_currency','sell_currency','buy_amount','sell_amount']
    order_obj = Order(**{f:order[f] for f in fields})
    order_obj.signature = signature
    g.session.add(order_obj)
    g.session.commit()
    pass

def fill_order(order,txes=[]):
    fields = ['sender_pk','receiver_pk','buy_currency','sell_currency','buy_amount','sell_amount']
    order_obj = Order(**{f:order[f] for f in fields})
    unfilled_db = g.session.query(Order).filter(Order.filled == None).all()
    for existing_order in unfilled_db:       
        if existing_order.buy_currency == order_obj.sell_currency:
            if existing_order.sell_currency == order_obj.buy_currency:
                if (existing_order.sell_amount / existing_order.buy_amount) >= (order_obj.buy_amount/order_obj.sell_amount) :
                    existing_order.filled = datetime.now()
                    order_obj.filled = datetime.now()
                    existing_order.counterparty_id = order_obj.id
                    order_obj.counterparty_id = existing_order.id
                    print(order.timestamp)
                    print(order.counterparty[0].timestamp)
                    g.session.commit()
                    if (existing_order.buy_amount > order_obj.sell_amount) | (order_obj.buy_amount > existing_order.sell_amount) :
                        if (existing_order.buy_amount > order_obj.sell_amount):
                            parent = existing_order
                            counter = order_obj
                        if order_obj.buy_amount > existing_order.sell_amount:
                            parent = order_obj
                            counter = existing_order
                        child = {}
                        child['sender_pk'] = parent.sender_pk
                        child['receiver_pk'] = parent.receiver_pk
                        child['buy_currency'] = parent.buy_currency
                        child['sell_currency'] = parent.sell_currency
                        child['buy_amount'] = parent.buy_amount-counter.sell_amount
                        child['sell_amount'] = (parent.buy_amount-counter.sell_amount)*(parent.sell_amount/parent.buy_amount)  
                        child_obj = Order(**{f:child[f] for f in fields})
                        child_obj.creator_id = parent.id
                        g.session.add(child_obj)
                        g.session.commit()
                        break
                    break

    g.session.commit()
        
    pass
  
def log_message(d):
    # Takes input dictionary d and writes it to the Log table
    # Hint: use json.dumps or str() to get it in a nice string form
    # Takes input dictionary d and writes it to the Log table
    #g.session.query(Log).all()
    #print(json.dumps(d))
    log_obj = Log()
    log_obj.message = json.dumps(d)
    #log = g.session.get('log')
    g.session.add(log_obj)
    g.session.commit()
    pass

def order_asdict(order):
    return {'sender_pk': order.sender_pk,'receiver_pk': order.receiver_pk, 'buy_currency': order.buy_currency, 'sell_currency': order.sell_currency, 'buy_amount': order.buy_amount, 'sell_amount': order.sell_amount, 'signature':order.signature,'counterparty_id':order.counterparty_id}

""" End of helper methods """



@app.route('/trade', methods=['POST'])
def trade():
    print("In trade endpoint")
    if request.method == "POST":
        content = request.get_json(silent=True)
        print( f"content = {json.dumps(content)}" )
        columns = [ "sender_pk", "receiver_pk", "buy_currency", "sell_currency", "buy_amount", "sell_amount", "platform" ]
        fields = [ "sig", "payload" ]

        for field in fields:
            if not field in content.keys():
                print( f"{field} not received by Trade" )
                print( json.dumps(content) )
                log_message(content)
                return jsonify( False )
        
        for column in columns:
            if not column in content['payload'].keys():
                print( f"{column} not received by Trade" )
                print( json.dumps(content) )
                log_message(content)
                return jsonify( False )
            
        #Your code here
        #Note that you can access the database session using g.session
        sig = content.get('sig')
        payload = content.get('payload')
        # TODO: Check the signature
        if check_sig(payload,sig):
            process_order(content)
            order = content.get('payload')
            fill_order(order)
        
        # TODO: Be sure to return jsonify(True) or jsonify(False) depending on if the method was successful
        return jsonify(True)

@app.route('/order_book')
def order_book():
    #Your code here
    raw_db = g.session.query(Order).all()
    db = []
    for order in raw_db:
        db.append(order_asdict(order))
    #result = dict(data = db)
    result = {}
    result['data']=db
    #print(result)
    #Note that you can access the database session using g.session
    return jsonify(result)

if __name__ == '__main__':
    app.run(port='5002')