package examples;

use strict;
use warnings;
use FindBin;
use lib (
    $FindBin::Bin . '/../lib',
    $FindBin::Bin . '/../../net-amqp/lib',
);
use POE qw(Component::Client::AMQP);
use base qw(Exporter);

our @EXPORT = qw($amq $channel init);

# Libraries for the dumper() calls
use YAML::XS;
use Net::AMQP::Common qw(show_ascii);
use Term::ANSIColor qw(:constants);

my $debug = defined $ENV{DEBUG} ? $ENV{DEBUG} : 1;

Net::AMQP::Protocol->load_xml_spec($FindBin::Bin . '/../../net-amqp/spec/amqp0-8.xml');

our ($amq, $channel);

sub init {
    my (%args) = @_;

    $args{Callbacks}{Reconnected} = [
        sub {
            my $amq = shift;
            $amq->Logger->info("We have been reconnected");
        },
    ];
    
    $amq = POE::Component::Client::AMQP->create(
        RemoteAddress => [qw(127.0.0.1 127.0.0.2)],

        Reconnect     => 1,

        ($debug ? (
        Debug         => {
            logic        => 1,

            frame_input  => 1,
            frame_output => 1,
            frame_dumper => sub {
                my $output = YAML::XS::Dump(shift);
                chomp $output;
                return "\n" . BLUE . $output . RESET;
            },

            raw_input    => 1,
            raw_output   => 1,
            raw_dumper => sub {
                my $raw = shift;
                my $output = "raw [".length($raw)."]: ".show_ascii($raw);
                return "\n" . YELLOW . $output . RESET;
            },
        },
        ) : ()),
        %args,
    );

    $channel = $amq->channel();
}

1;
