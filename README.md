# Amp ElasticSearch Client

`webgriffe/amp-elasticsearch` is a non-blocking ElasticSearch client for use with the [`amp`](https://github.com/amphp/amp)
concurrency framework.

---

:arrow_up: Updated fork requiring **PHP 8.1+**, **AMPHP 5+** and tested with **Elasticsearch 8.12+**.

:star: Original project (for PHP<8): https://github.com/webgriffe/amp-elasticsearch :star:

---

[![Build Status](https://github.com/webgriffe/amp-elasticsearch/workflows/Build/badge.svg)](https://github.com/webgriffe/amp-elasticsearch/actions)

**Required PHP Version**

- PHP 8.1+

**Installation**

```bash
composer config repositories.nanawel-amp-elasticsearch vcs https://github.com/nanawel/amp-elasticsearch.git
composer require webgriffe/amp-elasticsearch:dev-master
```

**Usage**

Just create a client instance and call its public methods which returns promises:

```php
Loop::run(function () {
  $client = new Webgriffe\AmpElasticsearch\Client('http://my.elasticsearch.test:9200');
  yield $this->client->createIndex('myindex');
  $response = yield $this->client->indexDocument('myindex', '', ['testField' => 'abc']);
  echo $response['result']; // 'created'
});
```

See other usage examples in the [`tests/Integration/ClientTest.php`](./tests/Integration/ClientTest.php).

All client methods return an array representation of the ElasticSearch REST API responses in case of sucess or an `Webgriffe\AmpElasticsearch\Error` in case of error.

## License

The MIT License (MIT). Please see [`LICENSE`](./LICENSE) for more information.
