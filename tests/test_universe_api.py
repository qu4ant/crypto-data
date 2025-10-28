"""
API interaction and retry logic tests for universe ingestion.

Tests CoinMarketCap API calls, retry mechanisms, and error handling.
"""

import pytest
import time
from unittest.mock import patch, MagicMock, call
import requests

from crypto_data.clients.coinmarketcap import CoinMarketCapClient


class TestApiCallWithRetry:
    """Test the CoinMarketCapClient._call_with_retry() method."""

    def test_successful_request(self):
        """Test that successful request returns response immediately."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': []}

        client = CoinMarketCapClient()
        with patch('requests.get', return_value=mock_response) as mock_get:
            result = client._call_with_retry('https://api.test.com', {'param': 'value'}, timeout=10)

            # Should call once and return
            assert mock_get.call_count == 1
            assert result == mock_response

    def test_rate_limit_429_retries(self):
        """Test that 429 (rate limit) error triggers retry with 60s wait."""
        # First call fails with 429, second succeeds
        mock_response_429 = MagicMock()
        mock_response_429.status_code = 429
        mock_response_429.raise_for_status.side_effect = requests.HTTPError(response=mock_response_429)

        mock_response_200 = MagicMock()
        mock_response_200.status_code = 200
        mock_response_200.json.return_value = {'data': []}

        client = CoinMarketCapClient()
        with patch('requests.get', side_effect=[mock_response_429, mock_response_200]) as mock_get:
            with patch('time.sleep') as mock_sleep:
                result = client._call_with_retry('https://api.test.com', {}, timeout=10)

                # Should retry once
                assert mock_get.call_count == 2
                assert mock_sleep.call_count == 1
                assert mock_sleep.call_args[0][0] == 60  # Should wait 60 seconds
                assert result == mock_response_200

    def test_rate_limit_exhausts_retries(self):
        """Test that 429 error exhausts retries and raises exception."""
        mock_response_429 = MagicMock()
        mock_response_429.status_code = 429
        mock_response_429.raise_for_status.side_effect = requests.HTTPError(response=mock_response_429)

        client = CoinMarketCapClient()
        with patch('requests.get', return_value=mock_response_429):
            with patch('time.sleep'):
                with pytest.raises(requests.HTTPError):
                    client._call_with_retry('https://api.test.com', {}, timeout=10)

    def test_server_error_500_retries(self):
        """Test that 500 (server error) triggers retry with 5s wait."""
        mock_response_500 = MagicMock()
        mock_response_500.status_code = 500
        mock_response_500.raise_for_status.side_effect = requests.HTTPError(response=mock_response_500)

        mock_response_200 = MagicMock()
        mock_response_200.status_code = 200

        client = CoinMarketCapClient()
        with patch('requests.get', side_effect=[mock_response_500, mock_response_200]) as mock_get:
            with patch('time.sleep') as mock_sleep:
                result = client._call_with_retry('https://api.test.com', {}, timeout=10)

                assert mock_get.call_count == 2
                assert mock_sleep.call_count == 1
                assert mock_sleep.call_args[0][0] == 5  # Should wait 5 seconds
                assert result == mock_response_200

    def test_server_error_503_retries(self):
        """Test that 503 (service unavailable) triggers retry with 5s wait."""
        mock_response_503 = MagicMock()
        mock_response_503.status_code = 503
        mock_response_503.raise_for_status.side_effect = requests.HTTPError(response=mock_response_503)

        mock_response_200 = MagicMock()
        mock_response_200.status_code = 200

        client = CoinMarketCapClient()
        with patch('requests.get', side_effect=[mock_response_503, mock_response_200]) as mock_get:
            with patch('time.sleep') as mock_sleep:
                result = client._call_with_retry('https://api.test.com', {}, timeout=10)

                assert mock_get.call_count == 2
                assert mock_sleep.call_count == 1
                assert mock_sleep.call_args[0][0] == 5  # Should wait 5 seconds

    def test_client_error_400_no_retry(self):
        """Test that 400 (client error) does not retry and raises immediately."""
        mock_response_400 = MagicMock()
        mock_response_400.status_code = 400
        mock_response_400.raise_for_status.side_effect = requests.HTTPError(response=mock_response_400)

        client = CoinMarketCapClient()
        with patch('requests.get', return_value=mock_response_400) as mock_get:
            with patch('time.sleep') as mock_sleep:
                with pytest.raises(requests.HTTPError):
                    client._call_with_retry('https://api.test.com', {}, timeout=10)

                # Should only call once (no retry)
                assert mock_get.call_count == 1
                assert mock_sleep.call_count == 0  # No sleep

    def test_client_error_404_no_retry(self):
        """Test that 404 (not found) does not retry and raises immediately."""
        mock_response_404 = MagicMock()
        mock_response_404.status_code = 404
        mock_response_404.raise_for_status.side_effect = requests.HTTPError(response=mock_response_404)

        client = CoinMarketCapClient()
        with patch('requests.get', return_value=mock_response_404) as mock_get:
            with patch('time.sleep') as mock_sleep:
                with pytest.raises(requests.HTTPError):
                    client._call_with_retry('https://api.test.com', {}, timeout=10)

                # Should only call once (no retry)
                assert mock_get.call_count == 1
                assert mock_sleep.call_count == 0

    def test_network_timeout_no_retry(self):
        """Test that network timeout does not retry and raises immediately."""
        client = CoinMarketCapClient()
        with patch('requests.get', side_effect=requests.Timeout("Connection timeout")) as mock_get:
            with patch('time.sleep') as mock_sleep:
                with pytest.raises(requests.Timeout):
                    client._call_with_retry('https://api.test.com', {}, timeout=10)

                # Should only call once (no retry for timeouts)
                assert mock_get.call_count == 1
                assert mock_sleep.call_count == 0

    def test_connection_error_no_retry(self):
        """Test that connection error does not retry and raises immediately."""
        client = CoinMarketCapClient()
        with patch('requests.get', side_effect=requests.ConnectionError("Connection refused")) as mock_get:
            with patch('time.sleep') as mock_sleep:
                with pytest.raises(requests.ConnectionError):
                    client._call_with_retry('https://api.test.com', {}, timeout=10)

                # Should only call once (no retry for connection errors)
                assert mock_get.call_count == 1
                assert mock_sleep.call_count == 0

    def test_max_retries_with_alternating_errors(self):
        """Test retry behavior with multiple retry-able errors."""
        # 500 -> 429 -> 503 -> 200 (3 retries, should succeed)
        mock_500 = MagicMock()
        mock_500.status_code = 500
        mock_500.raise_for_status.side_effect = requests.HTTPError(response=mock_500)

        mock_429 = MagicMock()
        mock_429.status_code = 429
        mock_429.raise_for_status.side_effect = requests.HTTPError(response=mock_429)

        mock_503 = MagicMock()
        mock_503.status_code = 503
        mock_503.raise_for_status.side_effect = requests.HTTPError(response=mock_503)

        mock_200 = MagicMock()
        mock_200.status_code = 200

        client = CoinMarketCapClient()
        with patch('requests.get', side_effect=[mock_500, mock_429, mock_503, mock_200]) as mock_get:
            with patch('time.sleep') as mock_sleep:
                result = client._call_with_retry('https://api.test.com', {}, timeout=10)

                assert mock_get.call_count == 4  # Initial + 3 retries
                assert mock_sleep.call_count == 3
                assert result == mock_200


class TestApiGetHistoricalListings:
    """Test the CoinMarketCapClient.get_historical_listings() method."""

    def test_successful_api_call(self, sample_cmc_api_response):
        """Test successful API call returns data correctly."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_cmc_api_response

        client = CoinMarketCapClient()
        with patch.object(client, '_call_with_retry', return_value=mock_response):
            result = client.get_historical_listings('2024-01-01', 100)

            assert isinstance(result, list)
            assert len(result) == 3  # From sample_cmc_api_response
            assert result[0]['symbol'] == 'BTC'
            assert result[0]['cmcRank'] == 1

    def test_api_called_with_correct_params(self):
        """Test that API is called with correct parameters."""
        mock_response = MagicMock()
        mock_response.json.return_value = {'data': []}

        client = CoinMarketCapClient()
        with patch.object(client, '_call_with_retry', return_value=mock_response) as mock_api:
            client.get_historical_listings('2024-01-01', 100)

            # Verify API was called with correct params
            call_args = mock_api.call_args
            assert 'cryptocurrency/listings/historical' in call_args[0][0]  # URL
            params = call_args[0][1]  # Params
            assert params['date'] == '2024-01-01'
            assert params['limit'] == 100
            assert params['start'] == 1
            assert params['convertId'] == 2781  # USD
            assert params['sort'] == 'cmc_rank'
            assert params['sort_dir'] == 'asc'

    def test_empty_api_response(self, empty_cmc_api_response):
        """Test handling of empty API response."""
        mock_response = MagicMock()
        mock_response.json.return_value = empty_cmc_api_response

        client = CoinMarketCapClient()
        with patch.object(client, '_call_with_retry', return_value=mock_response):
            result = client.get_historical_listings('2024-01-01', 100)

            assert isinstance(result, list)
            assert len(result) == 0

    def test_malformed_api_response(self, malformed_cmc_api_response):
        """Test handling of malformed API response (missing 'data' key)."""
        mock_response = MagicMock()
        mock_response.json.return_value = malformed_cmc_api_response

        client = CoinMarketCapClient()
        with patch.object(client, '_call_with_retry', return_value=mock_response):
            result = client.get_historical_listings('2024-01-01', 100)

            # Should return empty list for malformed response
            assert isinstance(result, list)
            assert len(result) == 0

    def test_api_timeout_propagates(self):
        """Test that API timeout error propagates correctly."""
        client = CoinMarketCapClient()
        with patch.object(client, '_call_with_retry', side_effect=requests.Timeout):
            with pytest.raises(requests.Timeout):
                client.get_historical_listings('2024-01-01', 100)

    def test_api_rate_limit_propagates(self):
        """Test that API rate limit error propagates after exhausting retries."""
        mock_response = MagicMock()
        mock_response.status_code = 429

        client = CoinMarketCapClient()
        with patch.object(client, '_call_with_retry', side_effect=requests.HTTPError(response=mock_response)):
            with pytest.raises(requests.HTTPError):
                client.get_historical_listings('2024-01-01', 100)


class TestDateRangeValidation:
    """Test date range validation (future dates)."""

    @pytest.mark.slow
    def test_future_date_returns_error(self):
        """Test that requesting future date returns API error."""
        # Note: This would actually hit the real API
        # In practice, we mock this to avoid real API calls
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'status': {
                'error_code': '500',
                'error_message': 'Search query is out of range'
            }
        }

        client = CoinMarketCapClient()
        with patch.object(client, '_call_with_retry', return_value=mock_response):
            result = client.get_historical_listings('2099-01-01', 100)

            # Should return empty list or handle error gracefully
            assert isinstance(result, list)
            assert len(result) == 0


class TestApiPerformance:
    """Test API call performance characteristics."""

    def test_retry_delays_are_correct(self):
        """Test that retry delays match expected values (60s for 429, 5s for 500/503)."""
        mock_429 = MagicMock()
        mock_429.status_code = 429
        mock_429.raise_for_status.side_effect = requests.HTTPError(response=mock_429)

        mock_200 = MagicMock()
        mock_200.status_code = 200

        client = CoinMarketCapClient()
        with patch('requests.get', side_effect=[mock_429, mock_200]):
            with patch('time.sleep') as mock_sleep:
                start_time = time.time()
                client._call_with_retry('https://api.test.com', {}, timeout=10)

                # Verify sleep was called with 60 seconds for 429
                mock_sleep.assert_called_once_with(60)

    def test_no_delay_on_success(self):
        """Test that successful requests have no artificial delay."""
        mock_200 = MagicMock()
        mock_200.status_code = 200

        client = CoinMarketCapClient()
        with patch('requests.get', return_value=mock_200):
            with patch('time.sleep') as mock_sleep:
                client._call_with_retry('https://api.test.com', {}, timeout=10)

                # Should not sleep on success
                mock_sleep.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
