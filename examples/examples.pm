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

our @EXPORT = qw($amq $channel);

# Libraries for the dumper() calls
use YAML::XS;
use Net::AMQP::Common qw(show_ascii);
use Term::ANSIColor qw(:constants);

my $debug = $ENV{DEBUG} || 1;

Net::AMQP::Protocol->load_xml_spec($ARGV[0] || $FindBin::Bin . '/../../net-amqp/spec/amqp0-8.xml');

our $amq = POE::Component::Client::AMQP->create(
    RemoteAddress => '127.0.0.1',

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
);

our $channel = $amq->channel();
