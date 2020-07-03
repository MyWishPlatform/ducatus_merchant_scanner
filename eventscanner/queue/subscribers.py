from pubsub import pub

from eventscanner.monitors.payments.duc_payment_monitor import DucPaymentMonitor


pub.subscribe(DucPaymentMonitor.on_new_block_event, 'DUCATUS_MAINNET')
