"""
Order manager application
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
import config
import threading
from graphene import Schema
from event_management.handler_registry import HandlerRegistry
from orders.handlers.order_created_handler import OrderCreatedHandler
from orders.handlers.order_creation_failed_handler import OrderCreationFailedHandler
from orders.handlers.order_cancelled_handler import OrderCancelledHandler
from orders.handlers.saga_completed_handler import SagaCompletedHandler
from stocks.handlers.stock_decreased_handler import StockDecreasedHandler
from stocks.handlers.stock_decrease_failed_handler import StockDecreaseFailedHandler
from stocks.handlers.stock_increased_handler import StockIncreasedHandler
from payments.handlers.payment_created_handler import PaymentCreatedHandler
from payments.handlers.payment_creation_failed_handler import PaymentCreationFailedHandler
from payments.outbox_processor import OutboxProcessor
from orders.queries.order_event_consumer import OrderEventConsumer
from stocks.schemas.query import Query
from flask import Flask, request, jsonify
from orders.controllers.order_controller import create_order, remove_order, get_order, get_report_highest_spending_users, get_report_best_selling_products, update_order
from orders.controllers.user_controller import create_user, remove_user, get_user
from stocks.controllers.product_controller import create_product, remove_product, get_product
from stocks.controllers.stock_controller import get_stock, populate_redis_on_startup, set_stock, get_stock_overview

app = Flask(__name__)

is_outbox_processor_running = False
if not is_outbox_processor_running:
    OutboxProcessor().run()
    is_outbox_processor_running = True

# Auto-populate Redis 5s after API startup (to give enough time for the DB to start up as well)
thread = threading.Timer(10.0, populate_redis_on_startup)
thread.daemon = True
thread.start()

registry = HandlerRegistry()
registry.register(OrderCreatedHandler())
registry.register(OrderCreationFailedHandler())
registry.register(OrderCancelledHandler())
registry.register(StockDecreasedHandler())
registry.register(StockDecreaseFailedHandler())
registry.register(StockIncreasedHandler())
registry.register(PaymentCreatedHandler())
registry.register(PaymentCreationFailedHandler())
registry.register(SagaCompletedHandler())

# le consommateur peut écouter 1 ou plusieurs topics (str or array)
consumer_service = OrderEventConsumer(
    bootstrap_servers=config.KAFKA_HOST,
    topic=config.KAFKA_TOPIC,
    group_id=config.KAFKA_GROUP_ID,
    registry=registry
)
consumer_service.start()

@app.get('/health-check')
def health():
    """Return OK if app is up and running"""
    return jsonify({'status':'ok'})

# Write routes (Commands)
@app.post('/orders')
def post_orders():
    """Create a new order based on information on request body"""
    return create_order(request)

@app.delete('/orders/<int:order_id>')
def delete_orders_id(order_id):
    """Delete an order with a given order_id"""
    return remove_order(order_id)

@app.post('/products')
def post_products():
    """Create a new product based on information on request body"""
    return create_product(request)

@app.delete('/products/<int:product_id>')
def delete_products_id(product_id):
    """Delete a product with a given product_id"""
    return remove_product(product_id)

@app.post('/users')
def post_users():
    """Create a new user based on information on request body"""
    return create_user(request)

@app.delete('/users/<int:user_id>')
def delete_users_id(user_id):
    """Delete a user with a given user_id"""
    return remove_user(user_id)

@app.post('/stocks')
def post_stocks():
    """Set product stock based on information on request body"""
    return set_stock(request)

# Read routes (Queries) 
@app.get('/orders/<int:order_id>')
def get_order_id(order_id):
    """Get order with a given order_id"""
    return get_order(order_id)

@app.get('/products/<int:product_id>')
def get_product_id(product_id):
    """Get product with a given product_id"""
    return get_product(product_id)

@app.get('/users/<int:user_id>')
def get_user_id(user_id):
    """Get user with a given user_id"""
    return get_user(user_id)

@app.get('/stocks/<int:product_id>')
def get_stocks(product_id):
    """Get product stocks by product_id"""
    return get_stock(product_id)

@app.get('/orders/reports/highest-spenders')
def get_orders_highest_spending_users():
    """Get list of highest speding users, order by total expenditure"""
    rows = get_report_highest_spending_users()
    return jsonify(rows)

@app.get('/orders/reports/best-sellers')
def get_orders_report_best_selling_products():
    """Get list of best selling products, order by number of orders"""
    rows = get_report_best_selling_products()
    return jsonify(rows)

@app.get('/stocks/reports/overview-stocks')
def get_stocks_overview():
    """Get stocks for all products"""
    rows = get_stock_overview()
    return jsonify(rows)

# Endpoint that allows suppliers to check stock
@app.post('/stocks/graphql-query')
def graphql_supplier():
    data = request.get_json()
    schema = Schema(query=Query)
    result = schema.execute(data['query'], variables=data.get('variables'))
    return jsonify({
        'data': result.data,
        'errors': [str(e) for e in result.errors] if result.errors else None
    })

# Integration with payment service
@app.put('/orders')
def put_orders():
    """Update one or more order fields"""
    return update_order(request)

# Start Flask app
if __name__ == '__main__':
    """Main entry point for the consumer service"""
    app.run(host='0.0.0.0', port=5000)
