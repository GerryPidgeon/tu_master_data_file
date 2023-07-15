from rx_name_check import process_rx_name_data
from deliverect import process_deliverect_data
from uber_eats import process_uber_eats_order_data
from uber_eats import process_uber_eats_payment_details_data
from uber_eats import process_uber_eats_refund_data
from uber_eats import process_uber_eats_review_data
from uber_eats import process_uber_eats_consolidtated_data
from wolt import process_wolt_order_data
from lieferando import process_lieferando_data
from food_panda import process_food_panda_data
from consolidation import process_consolidated_data
from item_level_detail import process_item_level_data, process_item_level_analysis

def main():
    process_rx_name_data()
    process_deliverect_data()
    process_uber_eats_order_data()
    process_uber_eats_payment_details_data()
    process_uber_eats_refund_data()
    process_uber_eats_review_data()
    process_uber_eats_consolidtated_data()
    process_wolt_order_data()
    process_lieferando_data()
    process_food_panda_data()
    process_consolidated_data()
    process_item_level_data()
    process_item_level_analysis()

if __name__ == '__main__':
    main()