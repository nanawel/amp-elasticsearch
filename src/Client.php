<?php

declare(strict_types=1);

namespace Webgriffe\AmpElasticsearch;

use Amp\Http\Client\HttpClient;
use Amp\Http\Client\HttpClientBuilder;
use Amp\Http\Client\Request;
use Amp\Http\Client\Response;
use Amp\Future;
use function Amp\async;

class Client
{
    /**
     * @var string
     */
    private $baseUri;
    /**
     * @var HttpClient
     */
    private $httpClient;

    public function __construct(string $baseUri, HttpClient $httpClient = null)
    {
        $this->httpClient = $httpClient ?: HttpClientBuilder::buildDefault();
        $this->baseUri = rtrim($baseUri, '/');
    }

    public function createIndex(string $index): Future
    {
        $method = 'PUT';
        $uri = implode('/', [$this->baseUri, urlencode($index)]);
        return $this->doJsonRequest($method, $uri);
    }

    public function existsIndex(string $index): Future
    {
        $method = 'HEAD';
        $uri = implode('/', [$this->baseUri, urlencode($index)]);
        return $this->doJsonRequest($method, $uri);
    }

    public function getIndex(string $index): Future
    {
        $method = 'GET';
        $uri = implode('/', [$this->baseUri, urlencode($index)]);
        return $this->doJsonRequest($method, $uri);
    }

    public function deleteIndex(string $index): Future
    {
        $method = 'DELETE';
        $uri = implode('/', [$this->baseUri, urlencode($index)]);
        return $this->doJsonRequest($method, $uri);
    }

    public function statsIndex(string $index, string $metric = '_all', array $options = []): Future
    {
        $method = 'GET';
        $uri = implode('/', [$this->baseUri, urlencode($index), '_stats', $metric]);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri);
    }

    public function indexDocument(
        string $index,
        string $id,
        array $body,
        array $options = [],
        string $type = '_doc'
    ): Future {
        $method = $id === '' ? 'POST' : 'PUT';
        $uri = implode('/', [$this->baseUri, urlencode($index), urlencode($type), urlencode($id)]);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doRequest($this->createJsonRequest($method, $uri, json_encode($body)));
    }

    public function existsDocument(string $index, string $id, string $type = '_doc'): Future
    {
        $method = 'HEAD';
        $uri = implode('/', [$this->baseUri, urlencode($index), urlencode($type), urlencode($id)]);
        return $this->doJsonRequest($method, $uri);
    }

    public function getDocument(string $index, string $id, array $options = [], string $type = '_doc'): Future
    {
        $method = 'GET';
        $uri = implode('/', [$this->baseUri, urlencode($index), urlencode($type), urlencode($id)]);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri);
    }

    public function deleteDocument(string $index, string $id, array $options = [], string $type = '_doc'): Future
    {
        $method = 'DELETE';
        $uri = implode('/', [$this->baseUri, urlencode($index), urlencode($type), urlencode($id)]);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri);
    }

    public function uriSearchOneIndex(string $index, string $query, array $options = []): Future
    {
        return $this->uriSearch($index, $query, $options);
    }

    public function uriSearchManyIndices(array $indices, string $query, array $options = []): Future
    {
        return $this->uriSearch(implode(',', $indices), $query, $options);
    }

    public function uriSearchAllIndices(string $query, array $options = []): Future
    {
        return $this->uriSearch('_all', $query, $options);
    }

    public function catIndices(string $index = null, array $options = []): Future
    {
        $method = 'GET';
        $uri = [$this->baseUri, '_cat', 'indices'];
        if ($index) {
            $uri[] = urlencode($index);
        }
        $uri = implode('/', $uri);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri);
    }

    public function catHealth(array $options = []): Future
    {
        $method = 'GET';
        $uri = implode('/', [$this->baseUri, '_cat', 'health']);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri);
    }

    public function refresh(string $indexOrIndices = null, array $options = []): Future
    {
        $method = 'POST';
        $uri = [$this->baseUri];
        if ($indexOrIndices) {
            $uri[] = urlencode($indexOrIndices);
        }
        $uri[] = '_refresh';
        $uri = implode('/', $uri);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri);
    }

    public function search(array $query, ?string $indexOrIndices = null, array $options = []): Future
    {
        $method = 'POST';
        $uri = [$this->baseUri];
        if ($indexOrIndices) {
            $uri[] = urlencode($indexOrIndices);
        }
        $uri[] = '_search';
        $uri = implode('/', $uri);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doRequest($this->createJsonRequest($method, $uri, json_encode($query)));
    }

    public function count(string $index, array $options = [], array $query = null): Future
    {
        $method = 'GET';
        $uri = [$this->baseUri, $index];
        $uri[] = '_count';
        $uri = implode('/', $uri);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        if (null !== $query) {
            return $this->doRequest($this->createJsonRequest($method, $uri, json_encode($query)));
        }
        return $this->doRequest($this->createJsonRequest($method, $uri));
    }

	/**
     * @param array|string $body
     * @param string|null $index
     * @return Future
     */
    public function bulk($body, string $index = null, array $options = []): Future
    {
        $method = 'POST';
        $uri = [$this->baseUri];
        if ($index) {
            $uri[] = urlencode($index);
        }
        $uri[] = '_bulk';
        $uri = implode('/', $uri);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doRequest(
            $this->createJsonRequest($method, $uri, (is_array($body) ? implode(PHP_EOL, array_map('json_encode', $body)) : $body) . PHP_EOL)
        );
    }

    public function updateByQuery(array $body, $indexOrIndices = null, array $options = []): Future {
        $method = 'POST';
        $uri = [$this->baseUri];
        if ($indexOrIndices) {
            $uri[] = urlencode((string) $indexOrIndices);
        }
        $uri[] = '_update_by_query';
        $uri = implode('/', $uri);
        if ($options) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doRequest($this->createJsonRequest($method, $uri, json_encode($body)));
    }

    private function createJsonRequest(string $method, string $uri, string $body = null): Request
    {
        $request = new Request($uri, $method);
        $request->setHeader('Content-Type', 'application/json');
        $request->setHeader('Accept', 'application/json');
        $request->setBodySizeLimit(15000000);

        if ($body) {
            $request->setBody($body);
        }
        return $request;
    }

    private function doRequest(Request $request): Future
    {
        return async(function () use ($request) {
            /** @var Response $response */
            $response = $this->httpClient->request($request);
            $body = $response->getBody()->buffer();
            $statusClass = (int) ($response->getStatus() / 100);
            if ($statusClass !== 2) {
                throw new Error($body, $response->getStatus());
            }
            if ($body === '') {
                return null;
            }
            return json_decode($body, true);
        });
    }

    private function uriSearch(string $indexOrIndicesOrAll, string $query, array $options): Future
    {
        $method = 'GET';
        $uri = implode('/', [$this->baseUri, urlencode($indexOrIndicesOrAll), urlencode('_search')]);
        if (!empty($query)) {
            $options['q'] = $query;
        }
        if (!empty($options)) {
            $uri .= '?' . http_build_query($options);
        }
        return $this->doJsonRequest($method, $uri);
    }

    /**
     * @return Future
     */
    private function doJsonRequest(string $method, string $uri): Future
    {
        return $this->doRequest($this->createJsonRequest($method, $uri));
    }
}
