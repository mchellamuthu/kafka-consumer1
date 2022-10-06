<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;

use Junges\Kafka\Facades\Kafka;
use Junges\Kafka\Contracts\KafkaConsumerMessage;

class ConsumeCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'kafka:consume';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Command description';

    /**
     * Execute the console command.
     *
     * @return int
     */
    public function handle()
    {
        $consumer = Kafka::createConsumer(['test_topic'])->withSasl(new \Junges\Kafka\Config\Sasl(
            password: config('kafka.secret'),
            username: config('kafka.key'),
            mechanisms: 'PLAIN',
            securityProtocol: 'SASL_SSL',
        ))
            ->withAutoCommit()
            ->withHandler(function (KafkaConsumerMessage $message) {
                // handle the payload
                $body =  $message->getBody();
                dump($body);
            })->build();
        $message = $consumer->consume();
    }
}
