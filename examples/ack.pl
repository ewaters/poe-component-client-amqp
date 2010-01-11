#!/usr/bin/perl

use strict;
use warnings;
use examples;

init();

# Create a queue, 'awesome', that will exist after this program ends
my $queue = $channel->queue(
    'awesome',
    {
        auto_delete => 0, # will remain after all consumers part
        exclusive   => 0, # not limited to just this connection
    },
);

# Publish a few things
$queue->publish('Totally rad 1');
$queue->publish('Totally rad 2');
$queue->publish('Totally rad 3');

my $i = 0;

$queue->subscribe(
    sub {
        my ($message, $meta) = @_;

        if (++$i == 3) {
            $amq->Logger->info("Shutting down...");
            $amq->stop();
        }

        if ($amq->is_stopping) {
            $amq->Logger->info("Got $message (ignored, redelivered later)");
            return 0; # don't ack it
        }
        else {
            $amq->Logger->info("Got $message");
            return 1; # ack it
        }
    },
    {
        no_ack => 0,
    }
);

$amq->run();

__DATA__

Got Totally rad 1
Got Totally rad 2
Shutting down...
Got Totally rad 3 (ignored, redelivered later)

    and upon a second run:

Got Totally rad 3
Got Totally rad 1
Shutting down...
Got Totally rad 2 (ignored, redelivered later)
Got Totally rad 3 (ignored, redelivered later)

