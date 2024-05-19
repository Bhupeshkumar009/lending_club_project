from lib.DataReader import  read_customers, read_orders
from lib.DataManipulation import filter_closed_orders, count_orders_state, filter_orders_generic
from lib.ConfigReader import get_app_config
import pytest

def test_read_customers_df(spark):
    df_cust=read_customers(spark,"LOCAL")
    cust_count=df_cust.count()
    assert cust_count==12435


def test_read_orders_df(spark):
    df_order=read_orders(spark,"LOCAL")
    orders_count=df_order.count()
    assert orders_count==68884

@pytest.mark.transformation()
def test_filter_closed_orders(spark):
    df_order=read_orders(spark, "LOCAL")
    closed_order=filter_closed_orders(df_order)
    closed_count=closed_order.count()
    assert closed_count == 7556
@pytest.mark.skip("work in progress")
def test_get_app_config():
    config=get_app_config("LOCAL")
    assert config["orders.file.path"]=="data/orders.csv"

@pytest.mark.transformation()
def test_count_orders_state(spark, expected_results):
    customers_df=read_customers(spark,"LOCAL")
    actual_results=count_orders_state(customers_df)
    assert actual_results.collect()==expected_results.collect()

@pytest.mark.skip()
def test_check_closed_count(spark):
    df_order=read_orders(spark,"LOCAL")
    filtered_count=filter_orders_generic(df_order,"CLOSED").count()
    assert filtered_count==7556    

@pytest.mark.skip()
def test_check_pending_payment_count(spark):
    df_order=read_orders(spark,"LOCAL")
    filtered_count=filter_orders_generic(df_order,"PENDING_PAYMENT").count()
    assert filtered_count==15030  

@pytest.mark.skip()
def test_check_COMPLETE_count(spark):
    df_order=read_orders(spark,"LOCAL")
    filtered_count=filter_orders_generic(df_order,"COMPLETE").count()
    assert filtered_count==22900

@pytest.mark.parametrize(
        "status,count",
        [("CLOSED",7556),
         ("PENDING_PAYMENT",15030),
         ("COMPLETE",22900)
         ]
)

@pytest.mark.latest
def test_check_count(spark,status,count):
    df_order=read_orders(spark,"LOCAL")
    filtered_count=filter_orders_generic(df_order,status).count()
    assert filtered_count==count

