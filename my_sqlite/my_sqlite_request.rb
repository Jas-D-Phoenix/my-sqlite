require 'csv'
class MySqliteRequest
  ORDER = %w[DESC ASC].freeze

  attr_reader :query_type

  def initialize(table_name = nil)
    # Data for forming the query
    @table_name = table_name
    @table_name << '.csv' unless @table_name.nil? || @table_name.end_with?('.csv')
    @query_type = nil
    @joins = []
    @wheres = []
    @select = []
    @order = []
    @values = nil
    @set = nil
    # Data for executing the query
    @table = nil
    @columns = nil
    @id = nil
  end

  def run
    run_errors
    @table, @columns = form_table(@table_name)
    check_column_mismatch
    join_tables
    case @query_type
    when :select
      run_select
    when :insert
      run_insert
    when :update
      run_update
    when :delete
      run_delete
    end
  end

  # Takes a csv file name
  def from(table_name)
    raise "Can't have two FROMs" if @table_name

    @table_name = table_name
    @table_name << '.csv' unless @table_name.nil? || @table_name.end_with?('.csv')
    self
  end

  def select(*column_names)
    @query_type ||= :select
    one_query_type(:select)

    @select.concat(column_names.map(&:to_sym))
    self
  end

  # Criteria can be a string or an array of strings
  def where(column_name, criteria)
    @wheres << [column_name.to_sym, criteria]
    self
  end

  def join(col_a, table_name_b, col_b)
    table_name_b << '.csv' unless table_name_b.nil? || table_name_b.end_with?('.csv')
    @joins << [col_a.to_sym, table_name_b, col_b.to_sym]
    self
  end

  def order(column_name, order = nil)
    order = 'ASC' if order.nil?
    raise 'Order must be ASC or DESC' unless ORDER.include?(order.upcase)

    @order << [order.upcase.to_sym, column_name.to_sym]
    self
  end

  def insert(table_name)
    raise "Can't have two FROMs" if @table_name
    @table_name = table_name
    @table_name << '.csv' unless @table_name.nil? || @table_name.end_with?('.csv') 
    @query_type ||= :insert
    one_query_type(:insert)
    self
  end

  def values(*data)
    raise "Can't have multiple values" if @values
    raise "Can't have value and set" if @set

    @values = data
    self
  end

  def update(table_name)
    raise "Can't have two FROMs" if @table_name

    @table_name = table_name
    @table_name << '.csv' unless @table_name.nil? || @table_name.end_with?('.csv') 
    @query_type ||= :update
    one_query_type(:update)
    self
  end

  def set(data)
    raise "Can't have multiple sets" if @set
    raise "Can't have value and set" if @values

    @set = data.transform_keys(&:to_sym)
    self
  end

  def delete
    @query_type ||= :delete
    one_query_type(:delete)
    self
  end

  private

  def one_query_type(query_type)
    raise "Can't have different query types" unless @query_type == query_type
  end

  def run_errors
    raise 'Must have a table!' unless @table_name
    raise 'Must have a query type!' unless @query_type
    raise 'Order and join can only be used with select!' if @query_type != :select &&
                                                            (@order.any? || @joins.any?)
    raise 'Insert must have values!' if @query_type == :insert && !@values
    raise 'Update must have set!' if @query_type == :update && !@set
    raise "Insert can't have where!" if @query_type == :insert && @wheres.any?
    raise "Select can't have values or set" if @query_type == :select && (@values || @set)
  end

  def check_column_mismatch
    raise "Insert column value mismatch!" if @query_type == :insert && @columns.size != (@values.size + 1)
  end

  def form_table(table_name)
    table = {}
    columns = nil
    File.foreach(table_name).with_index do |line, idx|
      if idx.zero?
        columns = line.chomp.split(',').map(&:to_sym) 
        columns << :id
      else
        row = {}
        CSV.parse_line(line).each_with_index { |val, i| row[columns[i]] = val }
        row[:id] = idx
        table[idx] = row
      end
    end
    @id = table.size + 1
    [table, columns]
  end

  def join_tables
    @joins.each do |join|
      col_a = join[0]
      table_b, columns_b = form_table(join[1])
      col_b = join[2]

      join_table(col_a, col_b, table_b, columns_b)
    end
  end

  def join_table(col_a, col_b, table_b, columns_b)
    @columns = (@columns + columns_b).uniq
    new_table = {}
    i = 1
    @table.each do |row_id, _v|
      table_b.each do |rowb_id, _v|
        if @table[row_id][col_a] == table_b[rowb_id][col_b]
          new_table[i] = @table[row_id].merge(table_b[rowb_id])
          new_table[i][:id] = i
          i += 1
        end
      end
    end
    @table = new_table
  end

  def run_select
    filtered = select_where
    if @select.include?(:*)
      selected = filtered.map { |_id, row| row }
    else
      selected = filtered.map { |_id, row| row.select { |k, _v| @select.include?(k) } }
    end
    order_table(selected)
  end

  def select_where
    return @table if @wheres.empty?

    @table.select do |_, row|
      check_where(row)
    end
  end

  def order_table(table)
    return table if @order.empty?

    table.sort do |a, b|
      res = 0
      @order.each do |order, column|
        res = sort_column(a[column], b[column], order)
        break unless res.zero?
      end
      res
    end
  end

  def sort_column(a, b, order)
    if order == :ASC
      a <=> b
    else
      b <=> a
    end
  end

  def run_insert
    new_row = {}
    @columns[0...-1].each.with_index { |col, i| new_row[col] = @values[i] }
    @table[@id] = new_row
    @id += 1
    change_db
  end

  def run_delete
    if @wheres.empty?
      @table = {}
    else
      delete_rows
    end
    change_db
  end

  def delete_rows
    @table.each do |id, row|
      @table.delete(id) if check_where(row)
    end
  end

  def run_update
    if @wheres.empty?
      @table.each { |_id, row| update_row(row) }
    else
      @table.select do |_, row|
        update_row(row) if check_where(row)
      end
    end
    change_db
  end

  def check_where(row)
    @wheres.all? do |where| 
      if where[1].is_a? Array
        where[1].include?(row[where[0]])
      else
        row[where[0]] == where[1] 
      end
    end
  end

  def update_row(row)
    @set.each { |k, v| row[k] = v }
  end

  def change_db
    CSV.open(@table_name, 'w') do |csv|
      csv << @columns[0...-1]
      @table.each { |_id, row| csv << row.reject { |k, _v| k == :id }.values }
    end
  end
end