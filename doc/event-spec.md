## Event Specifications

### `B` (Begin Transaction)
Indicates the start of a logical transaction block.
* **Byte Layout:**
  * `[0]`: `B`
  * `[1..9]`: Final LSN of the transaction (64-bit int).
  * `[9..17]`: Timestamp of the transaction (64-bit int).
  * `[17..21]`: Transaction ID / XID (32-bit int).
* **Parser Action:** Clear the current Transaction Buffer. Store the current XID and Final LSN in memory.

### `R` (Relation)
Provides the schema definition for a specific table. Always arrives *before* any `I`, `U`, or `D` event for that table.
* **Byte Layout:**
  * `[0]`: `R`
  * `[1..5]`: Relation ID (32-bit int).
  * `[5..x]`: Namespace / Schema name (Null-terminated string).
  * `[x..y]`: Table name (Null-terminated string).
  * `[y]`: Replica Identity setting (1 byte).
  * `[y+1..y+3]`: Number of columns (16-bit int).
  * *Loop per column:* 
    * `[0]`: Column flags (1 byte, indicates if it's a primary key).
    * `[1..z]`: Column name (Null-terminated string).
    * `[z..z+4]`: Postgres Type OID (32-bit int).
    * `[z+4..z+8]`: Type modifier (32-bit int).
* **Parser Action:** Parse the table name and column names. Upsert this definition into the **Relation Cache** using the Relation ID as the key.

### `I` (Insert)
A new row added to a table.
* **Byte Layout:**
  * `[0]`: `I`
  * `[1..5]`: Relation ID (32-bit int).
  * `[5]`: Tuple type `N`.
  * `[6..end]`: The tuple data block.
* **Parser Action:** 
  1. Fetch the column names from the Relation Cache using the Relation ID.
  2. Run the Tuple Parsing Subroutine on the `N` block.
  3. Zip the parsed data with the cached column names to create a JSON representation.
  4. Append `{ action: "INSERT", table: "...", data: {...} }` to the **Transaction Buffer**.

### `U` (Update)
An existing row modified in a table.
* **Byte Layout:**
  * `[0]`: `U`
  * `[1..5]`: Relation ID (32-bit int).
  * *Conditional Block 1:* May contain an `O` or `K` tuple block.
  * *Block 2:* Always contains an `N` tuple block.
* **Parser Action:**
  1. Fetch the column names from the Relation Cache.
  2. If an `O` or `K` block exists, parse it to extract the old primary key (crucial if the primary key itself was updated).
  3. Parse the `N` block to get the new row data.
  4. Append `{ action: "UPDATE", table: "...", old_key: {...}, data: {...} }` to the **Transaction Buffer**.

### `D` (Delete)
A row removed from a table.
* **Byte Layout:**
  * `[0]`: `D`
  * `[1..5]`: Relation ID (32-bit int).
  * `[5]`: Tuple type `O` or `K`.
  * `[6..end]`: The tuple data block.
* **Parser Action:**
  1. Fetch the column names from the Relation Cache.
  2. Parse the `O` or `K` block to identify which row was deleted.
  3. Append `{ action: "DELETE", table: "...", old_key: {...} }` to the **Transaction Buffer**.

### `C` (Commit)
Finalizes the transaction block. This is the trigger to move data out of memory and into your target system.
* **Byte Layout:**
  * `[0]`: `C`
  * `[1..2]`: Flags (8-bit int, currently unused).
  * `[2..10]`: Commit LSN (64-bit int).
  * `[10..18]`: End LSN of the transaction (64-bit int).
  * `[18..26]`: Commit Timestamp (64-bit int).
* **Parser Action:**
  1. Take all events sitting in the **Transaction Buffer**.
  2. Apply your deterministic hashing algorithm to the routing keys (table name + primary key) to calculate the target Redis Stream partitions.
  3. Batch execute the `XADD` commands to Redis.
  4. **Critical:** Once Redis confirms the writes, update your global **LSN Tracker** to the Commit LSN, so the next keep-alive message sent back to Postgres will safely free the WAL files.
  5. Clear the Transaction Buffer.