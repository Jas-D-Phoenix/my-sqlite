require 'date'
require_relative 'my_sqlite_request'

class MySqliteCLI
  KEYWORDS = %w[select insert update delete from join where order values set]

  def initialize(database)
    @database = database

    start_message
    ARGV.clear
    run
  end

  def run
    while true
      print 'my_sqlite_cli>'
      input = gets.chomp
      break if input == 'quit'
      input_arr = parse_input(input)
      action(input_arr)
    end
  end

  private

  def action(input)
    req = MySqliteRequest.new
    until input.empty?
      case input[0].upcase
      when 'SELECT'
        handle_select(req, input)
      when 'INSERT'
        handle_insert(req, input)
      when 'UPDATE'
        handle_update(req, input)
      when 'DELETE'
        handle_delete(req, input)
      when 'FROM'
        handle_from(req, input)
      when 'JOIN'
        handle_join(req, input)
      when 'WHERE'
        handle_where(req, input)
      when 'ORDER'
        handle_order(req, input)
      when 'VALUES'
        handle_values(req, input)
      when 'SET'
        handle_set(req, input)
      else
        raise 'Invalid syntax'
      end
    end
    run_query(req)
  end

  def handle_from(req, input)
    remove_extra_quotations(input[1])
    req.from(input[1])
    input.shift(2)
  end

  def handle_select(req, input)
    column_names = []
    while true
      input.shift
      if input.first.end_with?(',')
        column_names << input.first[0...-1]
      else
        column_names << input.first
        break
      end
    end
    input.shift
    column_names.each { |col| remove_extra_quotations(col) }
    req.select(*column_names)
  end

  def handle_insert(req, input)
    raise "Must be INSERT INTO" unless input[1].upcase == 'INTO'

    remove_extra_quotations(input[2])
    req.insert(input[2])
    input.shift(3)
  end

  def handle_update(req, input)
    remove_extra_quotations(input[1])
    req.update(input[1])
    input.shift(2)
  end

  def handle_delete(req, input)
    req.delete
    input.shift
  end

  def handle_join(req, input)
    raise 'Invalid join syntax' if input[5].nil? || 
                                   input[2].upcase != 'ON' || 
                                   input[4] != '='
            
    table_b = input[1]
    col_a = input[3]
    col_b = input[5]
    [col_a, table_b, col_b].each { |col| remove_extra_quotations(col) }
    req.join(col_a, table_b, col_b)
    input.shift(6)
  end

  def handle_order(req, input)
    raise 'Invalid order syntax' if input[1].upcase != 'BY' || input[2].nil?

    col_name = remove_extra_quotations(input[2])
    shift = 3
    order = remove_extra_quotations(input[3].upcase) if input[3]
    if %w[ASC DESC].include?(order)
      req.order(col_name, order)
      shift += 1
    else
      req.order(col_name)
    end
    input.shift(shift)
  end

  def handle_values(req, input)
    values = []
    raise 'Invalid values syntax' unless input[1] == '('
    input.shift
    while true
      input.shift
      raise 'Invalid values syntax' unless input[0][-1] == ',' || input[0][-1] == ')'

      values << remove_extra_quotations(input.first[0...-1])
      break unless input.first.end_with?(',')
    end
    input.shift
    req.values(*values)
  end

  def handle_set(req, input)
    data = {}
    input.shift
    while true
      col = input[0]
      raise 'Invalid syntax' unless input[1] == '='

      if input[2].end_with?(',')
        val = remove_extra_quotations(input[2][0...-1])
        data[col] = val
        input.shift(3)
      else
        val = remove_extra_quotations(input[2])
        data[col] = val
        input.shift(3)
        break
      end
    end
    req.set(data)
  end

  def handle_where(req, input)
    input.shift
    where_type = input[1].upcase
    if where_type == '='
      single_where(req, input)
    elsif where_type == 'IN'
      multi_where(req, input)
    else
      raise 'Invalid syntax'
    end
  end

  def single_where(req, input)
    col_name = input[0]
    criteria = input[2]
    raise 'Invalid where syntax' if criteria.nil?

    [col_name, criteria].each { |col| remove_extra_quotations(col) }
    req.where(col_name, criteria)
    input.shift(3)
  end

  # Where/Values has an extra skip since ( is separated
  def multi_where(req, input)
    criterias = []
    col_name = input.first
    raise 'Invalid where syntax' if input[2] != '('
    input.shift(3)

    while true
      raise 'Invalid values syntax' unless input[0][-1] == ',' || input[0][-1] == ')'

      criterias << remove_extra_quotations(input.first[0...-1])
      break unless input.first.end_with?(',')
    
      input.shift
    end
    input.shift
    remove_extra_quotations(col_name)
    req.where(col_name, criterias)
  end

  def run_query(req)
    req.query_type == :select ? output_query_results(req.run) : req.run
  end

  def output_query_results(results)
    results.each do |result| 
      result.values.each_with_index do |val, i|
        print val
        if i == (result.length - 1)
          print "\n"
        else
          print '|'
        end
      end
    end
  end

  def parse_input(input)
    final = input[-1]
    raise 'Query must end with semiclolon!' unless final == ';'
    line = add_space_to_left_paren(input[0...-1])
    CSV.parse_line(line, col_sep: ' ', quote_char: "'", liberal_parsing: true)
  end

  # Only does this if it's not inside of quotes
  def add_space_to_left_paren(line)
    stack = []
    line.each_char.with_index do |c, i|
      if c == "'"
        stack.empty? ? stack << c : stack.pop
      elsif c == '(' && stack.empty?
          line[i] = '( '
          break
      end
    end
    line
  end

  def remove_extra_quotations(col)
    col[0] = '' if col[0] == "'" || col[0] == '"'
    col[-1] = '' if col[-1] == "'" || col[-1] == '"'
    col
  end

  def start_message
    puts "MySQLite version 0.1 #{Date.today}"
  end
end

if __FILE__ == $PROGRAM_NAME
  MySqliteCLI.new(ARGV[0])
end