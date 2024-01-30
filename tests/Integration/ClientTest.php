<?php

declare(strict_types=1);

namespace Webgriffe\AmpElasticsearch\Tests\Integration;

use Webgriffe\AmpElasticsearch\Client;
use Webgriffe\AmpElasticsearch\Error;
use PHPUnit\Framework\TestCase;

class ClientTest extends TestCase
{
    final public const TEST_INDEX = 'test_index';
    final public const DEFAULT_ES_URL = 'http://127.0.0.1:9200';

    /**
     * @var Client
     */
    private $client;

    protected function setUp(): void
    {
        $esUrl = getenv('ES_URL') ?: self::DEFAULT_ES_URL;
        $this->client = new Client($esUrl);
        $indices = $this->client->catIndices()->await();
        foreach ($indices as $index) {
            $this->client->deleteIndex($index['index'])->await();
        }
    }

    public function testCreateIndex(): void
    {
        $response = $this->client->createIndex(self::TEST_INDEX)->await();
        $this->assertIsArray($response);
        $this->assertTrue($response['acknowledged']);
        $this->assertEquals(self::TEST_INDEX, $response['index']);
    }

    public function testIndicesExistsShouldThrow404ErrorIfIndexDoesNotExists(): void
    {
        $this->expectException(Error::class);
        $this->expectExceptionCode(404);
        $this->client->existsIndex(self::TEST_INDEX)->await();
    }

    public function testIndicesExistsShouldNotThrowAnErrorIfIndexExists(): void
    {
        $this->client->createIndex(self::TEST_INDEX)->await();
        $response = $this->client->existsIndex(self::TEST_INDEX)->await();
        $this->assertNull($response);
    }

    public function testDocumentsIndex(): void
    {
        $response = $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'])->await();
        $this->assertIsArray($response);
        $this->assertEquals(self::TEST_INDEX, $response['_index']);
    }

    public function testDocumentsIndexWithAutomaticIdCreation(): void
    {
        $response = $this->client->indexDocument(self::TEST_INDEX, '', ['testField' => 'abc'])->await();
        $this->assertIsArray($response);
        $this->assertEquals(self::TEST_INDEX, $response['_index']);
        $this->assertEquals('created', $response['result']);
    }

    public function testDocumentsExistsShouldThrowA404ErrorIfDocumentDoesNotExists(): void
    {
        $this->client->createIndex(self::TEST_INDEX)->await();
        $this->expectException(Error::class);
        $this->expectExceptionCode(404);
        $this->client->existsDocument(self::TEST_INDEX, 'not-existent-doc')->await();
    }

    public function testDocumentsExistsShouldNotThrowAnErrorIfDocumentExists(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'])->await();
        $response = $this->client->existsDocument(self::TEST_INDEX, 'my_id')->await();
        $this->assertNull($response);
    }

    public function testDocumentsGet(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'])->await();
        $response = $this->client->getDocument(self::TEST_INDEX, 'my_id')->await();
        $this->assertIsArray($response);
        $this->assertTrue($response['found']);
        $this->assertEquals('my_id', $response['_id']);
        $this->assertEquals('abc', $response['_source']['testField']);
    }

    public function testDocumentsGetWithOptions(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'])->await();
        $response = $this->client->getDocument(self::TEST_INDEX, 'my_id', ['_source' => 'false'])->await();
        $this->assertIsArray($response);
        $this->assertTrue($response['found']);
        $this->assertArrayNotHasKey('_source', $response);
    }

    public function testDocumentsGetWithOnlySource(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'])->await();
        $response = $this->client->getDocument(self::TEST_INDEX, 'my_id', [])->await();
        $this->assertIsArray($response);
        $this->assertEquals('abc', $response['_source']['testField']);
    }

    public function testDocumentsDelete(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'])->await();
        $response = $this->client->deleteDocument(self::TEST_INDEX, 'my_id')->await();
        $this->assertIsArray($response);
        $this->assertEquals('deleted', $response['result']);
    }

    public function testUriSearchOneIndex(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true'])
            ->await();
        $response = $this->client->uriSearchOneIndex(self::TEST_INDEX, 'testField:abc')->await();
        $this->assertIsArray($response);
        $this->assertCount(1, $response['hits']['hits']);
    }

    public function testUriSearchAllIndices(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true'])
            ->await();
        $response = $this->client->uriSearchAllIndices('testField:abc')->await();
        $this->assertIsArray($response);
        $this->assertCount(1, $response['hits']['hits']);
    }

    public function testUriSearchManyIndices(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true'])
            ->await();
        $response = $this->client->uriSearchManyIndices([self::TEST_INDEX], 'testField:abc')->await();
        $this->assertIsArray($response);
        $this->assertCount(1, $response['hits']['hits']);
    }

    public function testStatsIndexWithAllMetric(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true'])
            ->await();
        $response = $this->client->statsIndex(self::TEST_INDEX)->await();
        $this->assertEquals(1, $response['indices'][self::TEST_INDEX]['total']['indexing']['index_total']);
    }

    public function testStatsIndexWithDocsMetric(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true'])
            ->await();
        $response = $this->client->statsIndex(self::TEST_INDEX, 'docs')->await();
        $this->assertArrayNotHasKey('indexing', $response['indices'][self::TEST_INDEX]['total']);
        $this->assertEquals(1, $response['indices'][self::TEST_INDEX]['total']['docs']['count']);
    }

    public function testCatIndices(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true'])
            ->await();
        $response = $this->client->catIndices()->await();
        $this->assertCount(1, $response);
        $this->assertEquals(self::TEST_INDEX, $response[0]['index']);
    }

    public function testCatIndicesWithoutIndices(): void
    {
        $response = $this->client->catIndices()->await();
        $this->assertCount(0, $response);
    }

    public function testCatIndicesWithSpecificIndex(): void
    {
        $this->client->indexDocument(self::TEST_INDEX, 'my_id', ['testField' => 'abc'], ['refresh' => 'true'])
            ->await();
        $this->client->indexDocument('another_index', 'my_id', ['testField' => 'abc'], ['refresh' => 'true'])
            ->await();
        $response = $this->client->catIndices(self::TEST_INDEX)->await();
        $this->assertCount(1, $response);
        $this->assertEquals(self::TEST_INDEX, $response[0]['index']);
    }

    public function testCatHealth(): void
    {
        $response = $this->client->catHealth()->await();
        $this->assertCount(1, $response);
        $this->assertArrayHasKey('status', $response[0]);
    }

    public function testRefreshOneIndex(): void
    {
        $this->client->createIndex(self::TEST_INDEX)->await();
        $response = $this->client->refresh(self::TEST_INDEX)->await();
        $this->assertCount(1, $response);
    }

    public function testRefreshManyIndices(): void
    {
        $this->client->createIndex('an_index')->await();
        $this->client->createIndex('another_index')->await();
        $response = $this->client->refresh('an_index,another_index')->await();
        $this->assertCount(1, $response);
    }

    public function testRefreshAllIndices(): void
    {
        $this->client->createIndex(self::TEST_INDEX)->await();
        $response = $this->client->refresh()->await();
        $this->assertCount(1, $response);
    }

    public function testSearch(): void
    {
        $this->client->createIndex(self::TEST_INDEX)->await();
        $this->client->indexDocument(self::TEST_INDEX, 'document-id', ['uuid' => 'this-is-a-uuid', 'payload' => []], ['refresh' => 'true'])
            ->await();
        $query = [
            'query' => [
                'term' => [
                    'uuid.keyword' => [
                        'value' => 'this-is-a-uuid'
                    ]
                ]
            ]
        ];
        $response = $this->client->search($query)->await();
        $this->assertIsArray($response);
        $this->assertCount(1, $response['hits']['hits']);
    }

    public function testUpdateByQuery(): void
    {
        $this->client->createIndex(self::TEST_INDEX)->await();
        $this->client->indexDocument(self::TEST_INDEX, 'document-id', ['uuid' => 'this-is-a-uuid', 'payload' => '1'], ['refresh' => 'true'])
            ->await();
        $query = [
            'query' => [
                'term' => [
                    'uuid.keyword' => [
                        'value' => 'this-is-a-uuid'
                    ]
                ]
            ]
        ];
        $response = $this->client->search($query)->await();
        $this->assertIsArray($response);
        $this->assertCount(1, $response['hits']['hits']);
        $this->assertEquals('1', $response['hits']['hits'][0]['_source']['payload']);

        $this->client->updateByQuery(
            array_merge($query, ['script' => [
                'source' => 'ctx._source[\'payload\'] = \'2\'',
                'lang' => 'painless',
            ]]), self::TEST_INDEX, ['conflicts' => 'proceed']
        )->await();
        \Amp\delay(1);
        $response = $this->client->search($query)->await();
        $this->assertEquals('2', $response['hits']['hits'][0]['_source']['payload']);
    }

    public function testCount(): void
    {
        $this->client->createIndex(self::TEST_INDEX)->await();
        $this->client->indexDocument(self::TEST_INDEX, '', ['payload' => []], ['refresh' => 'true'])
            ->await();
        $this->client->indexDocument(self::TEST_INDEX, '', ['payload' => []], ['refresh' => 'true'])
            ->await();

        $response = $this->client->count(self::TEST_INDEX)->await();

        $this->assertIsArray($response);
        $this->assertEquals(2, $response['count']);
    }

    public function testCountWithQuery(): void
    {
        $this->client->createIndex(self::TEST_INDEX)->await();
        $this->client->indexDocument(self::TEST_INDEX, '', ['user' => 'kimchy'], ['refresh' => 'true'])
            ->await();
        $this->client->indexDocument(self::TEST_INDEX, '', ['user' => 'foo'], ['refresh' => 'true'])
            ->await();

        $response = $this->client->count(self::TEST_INDEX, [], ['query' => ['term' => ['user' => 'kimchy']]])->await();

        $this->assertIsArray($response);
        $this->assertEquals(1, $response['count']);
    }

    public function testBulkIndex(): void
    {
        $this->client->createIndex(self::TEST_INDEX)->await();
        $body = [];
        $responses = [];
        for ($i = 1; $i <= 1234; $i++) {
            $body[] = ['index' => ['_id' => $i]];
            $body[] = ['test' => 'bulk', 'my_field' => 'my_value_' .  $i];

            // Every 100 documents stop and send the bulk request
            if ($i % 100 === 0) {
                $responses = $this->client->bulk($body, self::TEST_INDEX)->await();
                $body = [];
                unset($responses);
            }
        }
        if (!empty($body)) {
            $responses = $this->client->bulk($body, self::TEST_INDEX)->await();
        }

        $this->assertTrue(isset($responses));
        $this->assertIsArray($responses);
        $this->assertCount(34, $responses['items']);
    }
}
