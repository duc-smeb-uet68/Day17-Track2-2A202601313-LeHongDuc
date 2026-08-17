# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Lê Hồng Đức **Lớp:** K3- Track 2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Output đầy đủ của một lần <code>make verify</code> gồm ba lượt chạy</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 27.1s
  run 2/3 … 30.0s
  run 3/3 … 28.6s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau retry/chạy lại, `gold_training_set` tăng lên 38.750 hàng thay vì 12.480 và nhiều `ticket_id` lặp lại. |
| **Nguyên nhân** | Model incremental không khai báo `unique_key`, nên dbt ghi theo kiểu append/`INSERT`. Retry ghi lại cùng tập ticket vào target thay vì thay thế hàng cũ; CDC có update khiến cách xoá theo partition ngày cũng không phù hợp với grain entity. |
| **Cách khắc phục** | Trong `dbt/models/gold/gold_training_set.sql`, đặt `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`, vẫn giữ điều kiện `_ingested_at` theo `run_date`. Trong `dags/ai_training_pipeline.py`, đặt `catchup=False`, `max_active_runs=1` để tránh schedule chạy bù và ghi đồng thời. |
| **Bằng chứng** | Trước: 38.750 hàng sau các lượt retry. Sau: 12.480 hàng, đúng 1 hàng/ticket. Checksum ba lượt đều `8dd7c98653`. |

`catchup` và `max_active_runs` chỉ giảm khả năng kích hoạt retry/backfill nguy hiểm; nguyên nhân gốc là phép ghi incremental không idempotent, nên phải sửa ở model.

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` có 8.645 thay vì 9.100 cặp `(event_date, customer_id)`; các cặp thiếu tập trung ở ngày sự kiện cũ. |
| **P99 độ trễ đo được** | **2.7258333333 ngày**. P50 = 0.1280902778 ngày, P95 = 1.8136927083 ngày, max = 2.9446875 ngày; 5.05% event đến muộn hơn một ngày. |
| **Lookback đã chọn** | **3 ngày** — là giá trị làm tròn lên từ P99, đồng thời bao phủ cả độ trễ lớn nhất của seed hiện tại. |
| **Nguyên nhân** | Điều kiện `event_date > max(event_date)` giả định event đến theo thứ tự thời gian sự kiện. Khi event cũ tới Bronze muộn, watermark event_date của Gold đã đi qua ngày đó nên record bị bỏ qua vĩnh viễn. |
| **Cách khắc phục** | Tính lại cửa sổ `event_date >= max(event_date) - interval 3 day`; cấu hình `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'delete+insert'` để bản tính lại thay thế đúng grain thay vì tạo bản sao. |
| **Bằng chứng** | Trước: 8.645 hàng. Sau: 9.100 hàng; checksum ba lượt đều `3db448685c`. |

Chọn P99 thay vì `max` giúp có mức bảo vệ định lượng trước phần lớn dữ liệu đến muộn mà không biến một outlier hiếm thành chi phí quét lại cố định ở mọi ngày. Lookback dài hơn làm tăng số ngày và số nhóm khách hàng phải aggregate lại trong mỗi lượt chạy. Với seed này, 3 ngày cũng đủ phủ max nên không phải đánh đổi thêm độ bao phủ.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Sau khi backend đổi cách biểu diễn từ số sang nhãn chuỗi, `try_cast` biến các nhãn hợp lệ thành `NULL`, trong khi vẫn cho `0`, `5`, `-1` đi qua. Baseline có 6.606 priority sai/NULL và quarantine rỗng. |
| **Nguyên nhân** | Silver chỉ dùng ép kiểu số nên không phân biệt schema evolution hợp lệ (`urgent/high/medium/low`) với payload thật sự lỗi; hơn nữa CDC được xếp hạng trước khi lọc, nên record lỗi mới nhất có thể loại cả ticket hợp lệ trước đó. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | (1) `1..4`: giữ nguyên; (2) `urgent/high/medium/low`: map lần lượt sang `1/2/3/4`; (3) `P1`, `P2`, `unknown`, `0`, `5`, `-1`, rỗng và `NULL`: trả về `NULL` rồi đưa vào quarantine. |
| **Cách khắc phục** | Viết macro `normalize_priority` chung; `silver_tickets` chuẩn hoá/lọc record `NULL` **trước** `row_number`, còn `quarantine_tickets` dùng chính macro đó để lấy record bị từ chối. Bật dbt contract cho `silver_tickets`, thêm `not_null` và `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312, `silver_tickets` vẫn đủ 12.480 ticket, priority chỉ thuộc 1..4 và `dbt test` = **11/11 pass**. Checksum quarantine ba lượt đều `ebb89036fb`. |

Bronze phải giữ payload nguyên gốc để có thể điều tra, replay và đối chiếu với nguồn; kiểm soát contract nên nằm ở Silver, nơi dữ liệu bắt đầu được chuẩn hoá cho consumer. Không nên dừng toàn DAG vì 312 record lỗi: tách chúng vào quarantine vừa không chặn hơn 130 nghìn event/31.200 chunk hợp lệ, vừa tạo hàng đợi có thể quan sát và xử lý lại.

---

## 4 · *(mở rộng)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A và B |
| **Nguyên nhân** | **A:** 5.000 Parquet file nhỏ, không partition, cộng với predicate `strftime(event_time, ...)` không sargable khiến DuckDB phải mở toàn bộ file. **B:** consumer commit offset trước khi ghi, vì vậy crash giữa hai thao tác tạo at-most-once và làm mất cả batch. |
| **Cách khắc phục** | **A:** compact vào 14 partition `event_date`, sắp xếp `customer_name, event_time`, row group 1.024; dashboard đọc Hive partition và filter `event_date = DATE '2026-08-09'`. **B:** ghi trước rồi mới commit offset; thêm primary key `event_id` và upsert `ON CONFLICT ... DO UPDATE` theo batch. `DO UPDATE` ghi nhận payload mới nếu message replay đã đổi, còn `DO NOTHING` sẽ giữ payload cũ. |
| **Bằng chứng** | **A:** rows scanned `5.000.000 → 9.324` (536,3×), files `5.000 → 14`, rows on disk giữ `130.683`, result hash giữ `4379e4c5d9f3`. **B:** crash ở batch 7 có offset `3.000`; sau restart có `20.000` hàng / `20.000` event_id, không mất, không trùng và `C == A`. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Grain thực tế, natural key và SQL incremental mà warehouse nhận khi retry/backfill. |
| 2 | Phân bố event time so với ingest time, watermark và chi phí của lookback window. |
| 3 | Ranh giới giữa schema evolution hợp lệ và dữ liệu lỗi, cùng vị trí quarantine trước bước dedup/CDC latest-state. |
