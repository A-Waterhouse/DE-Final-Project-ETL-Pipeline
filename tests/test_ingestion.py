from src.ingestion import ingestion
from unittest.mock import Mock, patch
import pytest
from src.ingestion import ingestion
import logging

@pytest.mark.describe('ingestion()')
@pytest.mark.it('test that we are using PG8000 to connect to db')
def test_ingestion_uses_pg8000_to_conect_to_DB(caplog):
    with patch('src.ingestion.wr.postgresql') as connection:
        with caplog.at_level(logging.WARNING):
            connection.connect.return_value = list()
            ingestion('event', 'context')
            assert 'Not pg8000 connection' in caplog.text


