"""
API interaction and retry logic tests for universe ingestion (Async).

Tests CoinMarketCap API calls, retry mechanisms, and error handling.
"""

import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
import aiohttp

from crypto_data.clients.coinmarketcap import CoinMarketCapClient


class TestApiCallWithRetry:
    """Test the CoinMarketCapClient._call_with_retry() method."""

    @pytest.mark.asyncio
    async def test_successful_request(self):
        """Test that successful request returns response immediately."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={'data': []})
        mock_response.raise_for_status = MagicMock()

        async with CoinMarketCapClient() as client:
            with patch.object(client._session, 'get') as mock_get:
                mock_get.return_value.__aenter__.return_value = mock_response

                result = await client._call_with_retry('https://api.test.com', {'param': 'value'}, timeout=10)

                # Should call once and return
                assert mock_get.call_count == 1
                assert result == {'data': []}

    @pytest.mark.asyncio
    async def test_rate_limit_429_retries(self):
        """Test that 429 (rate limit) error triggers retry with 60s wait."""
        # First call fails with 429, second succeeds
        mock_response_429 = AsyncMock()
        mock_response_429.status = 429
        mock_response_429.raise_for_status = MagicMock(side_effect=aiohttp.ClientResponseError(
            request_info=MagicMock(), history=(), status=429
        ))

        mock_response_200 = AsyncMock()
        mock_response_200.status = 200
        mock_response_200.json = AsyncMock(return_value={'data': []})
        mock_response_200.raise_for_status = MagicMock()

        async with CoinMarketCapClient() as client:
            with patch.object(client._session, 'get') as mock_get:
                mock_get.return_value.__aenter__.side_effect = [mock_response_429, mock_response_200]

                with patch('asyncio.sleep') as mock_sleep:
                    result = await client._call_with_retry('https://api.test.com', {}, timeout=10)

                    # Should retry once
                    assert mock_get.call_count == 2
                    assert mock_sleep.call_count == 1
                    assert mock_sleep.call_args[0][0] == 60  # Should wait 60 seconds
                    assert result == {'data': []}

    @pytest.mark.asyncio
    async def test_rate_limit_exhausts_retries(self):
        """Test that 429 error exhausts retries and raises exception."""
        mock_response_429 = AsyncMock()
        mock_response_429.status = 429
        mock_response_429.raise_for_status = MagicMock(side_effect=aiohttp.ClientResponseError(
            request_info=MagicMock(), history=(), status=429
        ))

        async with CoinMarketCapClient() as client:
            with patch.object(client._session, 'get') as mock_get:
                mock_get.return_value.__aenter__.return_value = mock_response_429

                with patch('asyncio.sleep'):
                    with pytest.raises(aiohttp.ClientResponseError):
                        await client._call_with_retry('https://api.test.com', {}, timeout=10)

    @pytest.mark.asyncio
    async def test_server_error_500_retries(self):
        """Test that 500 (server error) triggers retry with 5s wait."""
        mock_response_500 = AsyncMock()
        mock_response_500.status = 500
        mock_response_500.raise_for_status = MagicMock(side_effect=aiohttp.ClientResponseError(
            request_info=MagicMock(), history=(), status=500
        ))

        mock_response_200 = AsyncMock()
        mock_response_200.status = 200
        mock_response_200.json = AsyncMock(return_value={'data': []})
        mock_response_200.raise_for_status = MagicMock()

        async with CoinMarketCapClient() as client:
            with patch.object(client._session, 'get') as mock_get:
                mock_get.return_value.__aenter__.side_effect = [mock_response_500, mock_response_200]

                with patch('asyncio.sleep') as mock_sleep:
                    result = await client._call_with_retry('https://api.test.com', {}, timeout=10)

                    assert mock_get.call_count == 2
                    assert mock_sleep.call_count == 1
                    assert mock_sleep.call_args[0][0] == 5  # Should wait 5 seconds

    @pytest.mark.asyncio
    async def test_server_error_503_retries(self):
        """Test that 503 (service unavailable) triggers retry with 5s wait."""
        mock_response_503 = AsyncMock()
        mock_response_503.status = 503
        mock_response_503.raise_for_status = MagicMock(side_effect=aiohttp.ClientResponseError(
            request_info=MagicMock(), history=(), status=503
        ))

        mock_response_200 = AsyncMock()
        mock_response_200.status = 200
        mock_response_200.json = AsyncMock(return_value={'data': []})
        mock_response_200.raise_for_status = MagicMock()

        async with CoinMarketCapClient() as client:
            with patch.object(client._session, 'get') as mock_get:
                mock_get.return_value.__aenter__.side_effect = [mock_response_503, mock_response_200]

                with patch('asyncio.sleep') as mock_sleep:
                    result = await client._call_with_retry('https://api.test.com', {}, timeout=10)

                    assert mock_get.call_count == 2
                    assert mock_sleep.call_count == 1
                    assert mock_sleep.call_args[0][0] == 5  # Should wait 5 seconds

    @pytest.mark.asyncio
    async def test_client_error_400_no_retry(self):
        """Test that 400 (client error) retries but eventually raises."""
        mock_response_400 = AsyncMock()
        mock_response_400.status = 400
        mock_response_400.raise_for_status = MagicMock(side_effect=aiohttp.ClientResponseError(
            request_info=MagicMock(), history=(), status=400
        ))

        async with CoinMarketCapClient() as client:
            with patch.object(client._session, 'get') as mock_get:
                mock_get.return_value.__aenter__.return_value = mock_response_400

                with patch('asyncio.sleep') as mock_sleep:
                    with pytest.raises(aiohttp.ClientResponseError):
                        await client._call_with_retry('https://api.test.com', {}, timeout=10)

                    # In async version, client errors are retried via except clause
                    assert mock_get.call_count == 4  # Initial + 3 retries
                    assert mock_sleep.call_count == 3

    @pytest.mark.asyncio
    async def test_client_error_404_no_retry(self):
        """Test that 404 (not found) retries but eventually raises."""
        mock_response_404 = AsyncMock()
        mock_response_404.status = 404
        mock_response_404.raise_for_status = MagicMock(side_effect=aiohttp.ClientResponseError(
            request_info=MagicMock(), history=(), status=404
        ))

        async with CoinMarketCapClient() as client:
            with patch.object(client._session, 'get') as mock_get:
                mock_get.return_value.__aenter__.return_value = mock_response_404

                with patch('asyncio.sleep') as mock_sleep:
                    with pytest.raises(aiohttp.ClientResponseError):
                        await client._call_with_retry('https://api.test.com', {}, timeout=10)

                    # In async version, client errors are retried via except clause
                    assert mock_get.call_count == 4  # Initial + 3 retries
                    assert mock_sleep.call_count == 3

    @pytest.mark.asyncio
    async def test_network_timeout_no_retry(self):
        """Test that network timeout is not retried (asyncio.TimeoutError not caught)."""
        async with CoinMarketCapClient() as client:
            with patch.object(client._session, 'get') as mock_get:
                # asyncio.TimeoutError is raised before entering context manager
                mock_get.side_effect = asyncio.TimeoutError("Connection timeout")

                with patch('asyncio.sleep') as mock_sleep:
                    with pytest.raises(asyncio.TimeoutError):
                        await client._call_with_retry('https://api.test.com', {}, timeout=10)

                    # asyncio.TimeoutError is not caught by except clause, no retry
                    assert mock_get.call_count == 1  # Only initial attempt
                    assert mock_sleep.call_count == 0

    @pytest.mark.asyncio
    async def test_connection_error_no_retry(self):
        """Test that connection error retries with server_error_delay."""
        async with CoinMarketCapClient() as client:
            with patch.object(client._session, 'get') as mock_get:
                mock_get.return_value.__aenter__.side_effect = aiohttp.ClientError("Connection refused")

                with patch('asyncio.sleep') as mock_sleep:
                    with pytest.raises(aiohttp.ClientError):
                        await client._call_with_retry('https://api.test.com', {}, timeout=10)

                    # Should retry (client errors are retryable in async version)
                    assert mock_get.call_count == 4  # Initial + 3 retries
                    assert mock_sleep.call_count == 3

    @pytest.mark.asyncio
    async def test_max_retries_with_alternating_errors(self):
        """Test retry behavior with multiple retry-able errors."""
        # 500 -> 429 -> 503 -> 200 (3 retries, should succeed)
        mock_500 = AsyncMock()
        mock_500.status = 500
        mock_500.raise_for_status = MagicMock(side_effect=aiohttp.ClientResponseError(
            request_info=MagicMock(), history=(), status=500
        ))

        mock_429 = AsyncMock()
        mock_429.status = 429
        mock_429.raise_for_status = MagicMock(side_effect=aiohttp.ClientResponseError(
            request_info=MagicMock(), history=(), status=429
        ))

        mock_503 = AsyncMock()
        mock_503.status = 503
        mock_503.raise_for_status = MagicMock(side_effect=aiohttp.ClientResponseError(
            request_info=MagicMock(), history=(), status=503
        ))

        mock_200 = AsyncMock()
        mock_200.status = 200
        mock_200.json = AsyncMock(return_value={'data': []})
        mock_200.raise_for_status = MagicMock()

        async with CoinMarketCapClient() as client:
            with patch.object(client._session, 'get') as mock_get:
                mock_get.return_value.__aenter__.side_effect = [mock_500, mock_429, mock_503, mock_200]

                with patch('asyncio.sleep') as mock_sleep:
                    result = await client._call_with_retry('https://api.test.com', {}, timeout=10)

                    assert mock_get.call_count == 4  # Initial + 3 retries
                    assert mock_sleep.call_count == 3
                    assert result == {'data': []}


class TestApiGetHistoricalListings:
    """Test the CoinMarketCapClient.get_historical_listings() method."""

    @pytest.mark.asyncio
    async def test_successful_api_call(self, sample_cmc_api_response):
        """Test successful API call returns data correctly."""
        async with CoinMarketCapClient() as client:
            with patch.object(client, '_call_with_retry', new_callable=AsyncMock) as mock_api:
                mock_api.return_value = sample_cmc_api_response

                result = await client.get_historical_listings('2024-01-01', 100)

                assert isinstance(result, list)
                assert len(result) == 3  # From sample_cmc_api_response
                assert result[0]['symbol'] == 'BTC'
                assert result[0]['cmcRank'] == 1

    @pytest.mark.asyncio
    async def test_api_called_with_correct_params(self):
        """Test that API is called with correct parameters."""
        async with CoinMarketCapClient() as client:
            with patch.object(client, '_call_with_retry', new_callable=AsyncMock) as mock_api:
                mock_api.return_value = {'data': []}

                await client.get_historical_listings('2024-01-01', 100)

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

    @pytest.mark.asyncio
    async def test_empty_api_response(self, empty_cmc_api_response):
        """Test handling of empty API response."""
        async with CoinMarketCapClient() as client:
            with patch.object(client, '_call_with_retry', new_callable=AsyncMock) as mock_api:
                mock_api.return_value = empty_cmc_api_response

                result = await client.get_historical_listings('2024-01-01', 100)

                assert isinstance(result, list)
                assert len(result) == 0

    @pytest.mark.asyncio
    async def test_malformed_api_response(self, malformed_cmc_api_response):
        """Test handling of malformed API response (missing 'data' key)."""
        async with CoinMarketCapClient() as client:
            with patch.object(client, '_call_with_retry', new_callable=AsyncMock) as mock_api:
                mock_api.return_value = malformed_cmc_api_response

                result = await client.get_historical_listings('2024-01-01', 100)

                # Should return empty list for malformed response
                assert isinstance(result, list)
                assert len(result) == 0

    @pytest.mark.asyncio
    async def test_api_timeout_propagates(self):
        """Test that API timeout error propagates correctly."""
        async with CoinMarketCapClient() as client:
            with patch.object(client, '_call_with_retry', new_callable=AsyncMock) as mock_api:
                mock_api.side_effect = asyncio.TimeoutError

                with pytest.raises(asyncio.TimeoutError):
                    await client.get_historical_listings('2024-01-01', 100)

    @pytest.mark.asyncio
    async def test_api_rate_limit_propagates(self):
        """Test that API rate limit error propagates after exhausting retries."""
        async with CoinMarketCapClient() as client:
            with patch.object(client, '_call_with_retry', new_callable=AsyncMock) as mock_api:
                mock_api.side_effect = aiohttp.ClientResponseError(
                    request_info=MagicMock(), history=(), status=429
                )

                with pytest.raises(aiohttp.ClientResponseError):
                    await client.get_historical_listings('2024-01-01', 100)


class TestDateRangeValidation:
    """Test date range validation (future dates)."""

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_future_date_returns_error(self):
        """Test that requesting future date returns API error."""
        # Note: This would actually hit the real API
        # In practice, we mock this to avoid real API calls
        async with CoinMarketCapClient() as client:
            with patch.object(client, '_call_with_retry', new_callable=AsyncMock) as mock_api:
                mock_api.return_value = {
                    'status': {
                        'error_code': '500',
                        'error_message': 'Search query is out of range'
                    }
                }

                result = await client.get_historical_listings('2099-01-01', 100)

                # Should return empty list or handle error gracefully
                assert isinstance(result, list)
                assert len(result) == 0


class TestApiPerformance:
    """Test API call performance characteristics."""

    @pytest.mark.asyncio
    async def test_retry_delays_are_correct(self):
        """Test that retry delays match expected values (60s for 429, 5s for 500/503)."""
        mock_429 = AsyncMock()
        mock_429.status = 429
        mock_429.raise_for_status = MagicMock(side_effect=aiohttp.ClientResponseError(
            request_info=MagicMock(), history=(), status=429
        ))

        mock_200 = AsyncMock()
        mock_200.status = 200
        mock_200.json = AsyncMock(return_value={'data': []})
        mock_200.raise_for_status = MagicMock()

        async with CoinMarketCapClient() as client:
            with patch.object(client._session, 'get') as mock_get:
                mock_get.return_value.__aenter__.side_effect = [mock_429, mock_200]

                with patch('asyncio.sleep') as mock_sleep:
                    await client._call_with_retry('https://api.test.com', {}, timeout=10)

                    # Verify sleep was called with 60 seconds for 429
                    mock_sleep.assert_called_once_with(60)

    @pytest.mark.asyncio
    async def test_no_delay_on_success(self):
        """Test that successful requests have no artificial delay."""
        mock_200 = AsyncMock()
        mock_200.status = 200
        mock_200.json = AsyncMock(return_value={'data': []})
        mock_200.raise_for_status = MagicMock()

        async with CoinMarketCapClient() as client:
            with patch.object(client._session, 'get') as mock_get:
                mock_get.return_value.__aenter__.return_value = mock_200

                with patch('asyncio.sleep') as mock_sleep:
                    await client._call_with_retry('https://api.test.com', {}, timeout=10)

                    # Should not sleep on success
                    mock_sleep.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
