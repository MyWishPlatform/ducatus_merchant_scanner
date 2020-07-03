from eventscanner.queue.pika_handler import send_to_backend
from mywish_models.models import UserSiteBalance, session
from scanner.events.block_event import BlockEvent


class DucPaymentMonitor:
    network_type = ['DUCATUS_MAINNET']
    event_type = 'duc_transfer'

    @classmethod
    def on_new_block_event(cls, block_event: BlockEvent):
        if block_event.network.type not in cls.network_type:
            return

        addresses = block_event.transactions_by_address.keys()
        user_site_balances = session \
            .query(UserSiteBalance) \
            .filter(UserSiteBalance.duc_address.in_(addresses)) \
            .all()
        for usb in user_site_balances:
            transactions = block_event.transactions_by_address[usb.duc_address]

            for transaction in transactions:
                for output in transaction.outputs:
                    if usb.duc_address not in output.address:
                        print('{}: Found transaction out from internal address. Skip it.'
                              .format(block_event.network.type), flush=True)
                        continue

                    message = {
                        # 'userId': usb.user_id,
                        'txHash': transaction.tx_hash,
                        'currency': 'DUC',
                        'amount': output.value,
                        # 'siteId': usb.subsite_id,
                        'success': True,
                        'status': 'COMMITTED'
                    }

                    send_to_backend(cls.event_type, 'notification-ducatus-mainnet', message)
