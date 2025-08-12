package bench;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

@RestController
@RequestMapping("/test")
public class Controller {

    private final JdbcTemplate jdbc;
    private final DataSource ds;

    public Controller(JdbcTemplate jdbc, DataSource ds) {
        this.jdbc = jdbc;
        this.ds = ds;
    }

    /** 1) 단순 쿼리 + optional sleep */
    @GetMapping("/query")
    public Map<String, Object> query(@RequestParam(name="sleepSec", defaultValue="0") int sleepSec) {
        Instant t0 = Instant.now();
        try {
            if (sleepSec > 0) {
                // pg_sleep는 반환값이 없으므로 execute 형태로 수행
                jdbc.execute("select pg_sleep(?)", (PreparedStatement ps) -> {
                    ps.setInt(1, sleepSec);
                    ps.execute();
                    return null;
                });
            } else {
                jdbc.queryForObject("select 1", Integer.class);
            }
            long ms = Duration.between(t0, Instant.now()).toMillis();
            return Map.of("ok", true, "elapsed_ms", ms, "sleepSec", sleepSec);
        } catch (Exception e) {
            return Map.of("ok", false, "error", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    /** 2) 트랜잭션 점유(Idle in transaction 유발) */
    @PostMapping("/hold-tx")
    public Map<String, Object> holdTx(@RequestParam(name="holdSec", defaultValue="30") int holdSec,
                                      @RequestParam(name="commit", defaultValue="false") boolean commit) {
        Instant t0 = Instant.now();
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement("select pg_sleep(?)")) {
                ps.setInt(1, holdSec);
                ps.execute();
            }
            if (commit) c.commit(); else c.rollback();

            long ms = Duration.between(t0, Instant.now()).toMillis();
            return Map.of("ok", true, "held_sec", holdSec, "committed", commit, "elapsed_ms", ms);
        } catch (Exception e) {
            return Map.of("ok", false, "error", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    /** 3) 커넥션만 잡고 유지(풀 고갈 테스트: 쿼리 없이 점유) */
    @PostMapping("/hold-conn")
    public Map<String, Object> holdConn(@RequestParam(name="holdSec", defaultValue="30") int holdSec) {
        Instant t0 = Instant.now();
        try (Connection c = ds.getConnection()) {
            Thread.sleep(holdSec * 1000L);
            long ms = Duration.between(t0, Instant.now()).toMillis();
            return Map.of("ok", true, "held_sec", holdSec, "elapsed_ms", ms);
        } catch (Exception e) {
            return Map.of("ok", false, "error", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    /** 4) 풀 상태 간단 확인(현재 커넥션 획득 시간 측정) */
    @GetMapping("/ping")
    public Map<String, Object> ping() {
        Instant t0 = Instant.now();
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement("select 1");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            long ms = Duration.between(t0, Instant.now()).toMillis();
            return Map.of("ok", true, "acquire_and_query_ms", ms);
        } catch (Exception e) {
            return Map.of("ok", false, "error", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }
}
