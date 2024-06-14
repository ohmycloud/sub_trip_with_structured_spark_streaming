sub MAIN(Str :$station_id = 'ST231115539630001', Str :$host = '0.0.0.0', Int :$port = 3333, :$file) {
    my $sum = 0;

   # {"data_time": "2016-01-01 10:02:00" ,"station_id": "ST231115539630001"}
    react {
        whenever IO::Socket::Async.listen($host, $port) -> $conn {
          if $file.defined {
              my @lines = $file.IO.lines;
              for @lines -> $line {
                  print sprintf("%s\n", $line);
                  $conn.print: sprintf("%s\n", $line);
              }
          }

          react {
                my Bool:D $ignore = True;

                whenever Supply.interval(60).rotor(1, 1 => 1) {
                    $ignore = !$ignore;
                }

                whenever Supply.interval(1) {
                    next if $ignore;
                    $sum += 1;

                    my $now = DateTime.now;
                    my $event-time = sprintf("%s %s", $now.yyyy-mm-dd, $now.hh-mm-ss);

                    print sprintf("\{'station_id':'%s','data_time':'%s'}\n", $station_id, $event-time);
                    $conn.print: sprintf("\{'station_id':'%s','data_time':'%s'}\n", $station_id, $event-time);
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
