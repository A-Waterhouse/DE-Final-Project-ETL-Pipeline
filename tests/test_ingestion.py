from src.ingestion import ingestion, read_ingested_bucket_name
from src.utils.put_object_into_s3_bucket \
    import put_object_into_s3_bucket as put_s3
from src.utils.get_time_of_last_query import get_time_of_last_query as get_time
from unittest.mock import patch
import pytest
import logging
import boto3
import json
from moto import mock_aws
from datetime import datetime
from decimal import Decimal

@pytest.fixture
def s3_client():
    with mock_aws():
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.create_bucket(Bucket="terraform-12345", CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        yield s3

def test_read_ingested_bucket_name(s3_client):
    # Upload a test object
    s3_client.put_object(Bucket="terraform-12345", Key="tf-state", Body=json.dumps({"outputs": {"ingested_bucket": {"value": "my-bucket"}}}))
    
    
    result = read_ingested_bucket_name()
    
    assert result == "my-bucket"

@pytest.mark.describe("ingestion()")
@pytest.mark.it("test that we are using PG8000 to connect to db")
def test_ingestion_uses_pg8000_to_conect_to_DB(caplog):
    with patch("src.ingestion.wr.postgresql") as connection:
        with caplog.at_level(logging.WARNING):
            connection.connect.return_value = list()
            ingestion("event", "context")
            assert "Not pg8000 connection" in caplog.text


@pytest.mark.describe("ingestion()")
@pytest.mark.it("loggs_if_incorect_db_credentials")
@patch("src.ingestion.wr.postgresql")
@patch("src.ingestion.DB")
def test_ingestion_loggs_if_incorect_db_credentials(con, db, caplog):
    con.connect.return_value = list()
    ingestion("event", "context")
    assert "Not pg8000 connection" in caplog.text


@pytest.mark.describe("ingestion()")
@pytest.mark.it("ingestion write only JSON with data to s3")
@patch("src.ingestion.wr.postgresql")
@patch("src.ingestion.put_object_into_s3_bucket")
def test_ingestion_write_only_JSON_with_data_in_it(
        con, put_obj_into_s3_bucket):
    con.connect.return_value = list()
    con.run.return_value = list()
    con.close.return_value = "nothing"
    put_obj_into_s3_bucket.assert_not_called()
    
    

@pytest.mark.describe("put_object_into_s3_bucket()")
@pytest.mark.it("test function write into s3")
@mock_aws
def test_func_write_into_s3():
    data = {"some": "data", "to": "test"}
    s3 = boto3.resource("s3")
    s3.create_bucket(
        Bucket="mybucket",
        CreateBucketConfiguration={
            "LocationConstraint": "eu-west-2"})
    put_s3(data, "mybucket", "some_key")
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket="mybucket", Key="some_key.json")
    assert json.loads(obj["Body"].read().decode("utf-8")) == data


@pytest.mark.describe("put_object_into_s3_bucket()")
@pytest.mark.it("test function raise RuntimeError when NoSuchBucket")
@mock_aws
def test_RuntimeError_NoSuchBucket():
    with pytest.raises(RuntimeError, match="NoSuchBucket"):
        put_s3(None, "mybucket", "some_key")

