sub MAIN(Str :$host = '0.0.0.0', Int :$port = 3333) {

    my $id = '1';
    my $sum = 0;
    my $event-type = 'click';

   #  {"eventTime": "2016-01-01 10:02:00" ,"eventType": "click" ,"userId":"1"}
    react {
        whenever IO::Socket::Async.listen($host, $port) -> $conn {
            react {
                my Bool:D $ignore = True;

                whenever Supply.interval(60).rotor(1, 1 => 1) {
                    $ignore = !$ignore;
                }

                whenever Supply.interval(1) {
                    next if $ignore;
                    $sum += 1;
                    if $sum % 30 == 0 {
                       $event-type = "login";
                    } else {
                       $event-type = "click";
                    }

                    my $now = DateTime.now;
                    my $event-time = sprintf("%s %s", $now.yyyy-mm-dd, $now.hh-mm-ss);

                    print sprintf("\{'userId':'%s','eventType':'%s','eventTime':'%s'}\n", $id, $event-type, $event-time);
                    $conn.print: sprintf("\{'userId':'%s','eventType':'%s','eventTime':'%s'}\n", $id, $event-type, $event-time);
                }

                whenever signal(SIGINT) {
                    say "Done.";
                    done;
                }
            }
        }
        CATCH {
            default {
                say .^name, ': ', .Str;
                say "handled in $?LINE";
            }
        }
    }
}
