"""
Handler: Payment Creation Failed
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import config
from db import get_sqlalchemy_session
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer
from stocks.commands.write_stock import check_in_items_to_stock


class PaymentCreationFailedHandler(EventHandler):
    """Handles PaymentCreationFailed events"""
    
    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()
    
    def get_event_type(self) -> str:
        """Get event type name"""
        return "PaymentCreationFailed"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        """Execute every time the event is published"""
        try:
            session = get_sqlalchemy_session()
            check_in_items_to_stock(session, event_data['order_items'])
            session.commit()
            event_data['event'] = "StockIncreased"
        except Exception as e:
            session.rollback()
            event_data['event'] = "OrderCancelled"
            event_data['error'] = str(e)
        finally:
            session.close()
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
