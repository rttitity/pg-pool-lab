package bench;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import javax.sql.DataSource;
import java.sql.*;
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
    public Map<String, Object> query(@RequestParam(defaultValue = "0") int sleepSec) {
        Instant t0 = Instant.now();
        if (sleepSec > 0) {
            jdbc.queryForObject("select pg_sleep(?)", Long.class, (long) sleepSec);
        } else {
            jdbc.queryForObject("select 1", Integer.class);
        }
        long ms = Duration.between(t0, Instant.now()).toMillis();
        return Map.of("ok", true, "elapsed_ms", ms, "sleepSec", sleepSec);
    }

    /** 2) 트랜잭션 점유(Idle in transaction 유발) */
    @PostMapping("/hold-tx")
    public Map<String, Object> holdTx(@RequestParam(defaultValue = "30") int holdSec,
                                      @RequestParam(defaultValue = "false") boolean commit) throws Exception {
        Instant t0 = Instant.now();
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement("select pg_sleep(?)")) {
                ps.setLong(1, holdSec);
                ps.execute();
            }
            if (commit) c.commit(); else c.rollback();
        }
        long ms = Duration.between(t0, Instant.now()).toMillis();
        return Map.of("ok", true, "held_sec", holdSec, "committed", commit, "elapsed_ms", ms);
    }

    /** 3) 커넥션만 잡고 유지(풀 고갈 테스트: 쿼리 안 날리고 점유) */
    @PostMapping("/hold-conn")
    public Map<String, Object> holdConn(@RequestParam(defaultValue = "30") int holdSec) throws Exception {
        Instant t0 = Instant.now();
        try (Connection c = ds.getConnection()) {
            Thread.sleep(holdSec * 1000L);
        }
        long ms = Duration.between(t0, Instant.now()).toMillis();
        return Map.of("ok", true, "held_sec", holdSec, "elapsed_ms", ms);
    }

    /** 4) 풀 상태 간단 확인(현재 커넥션 획득 시간 측정) */
    @GetMapping("/ping")
    public Map<String, Object> ping() throws Exception {
        Instant t0 = Instant.now();
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement("select 1");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
        }
        long ms = Duration.between(t0, Instant.now()).toMillis();
        return Map.of("ok", true, "acquire_and_query_ms", ms);
    }
}
