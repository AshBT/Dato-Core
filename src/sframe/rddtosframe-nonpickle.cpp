/*
* Copyright (C) 2015 Dato, Inc.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#include <logger/logger.hpp>
#include <fileio/general_fstream.hpp>
#include <boost/tokenizer.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/concepts.hpp>
#include <boost/iostreams/operations.hpp> 
#include <boost/iostreams/device/file.hpp>
#include <sframe/sframe.hpp>
#include <sframe/csv_line_tokenizer.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <sframe/comma_escape_string.hpp>
#include <flexible_type/flexible_type_spirit_parser.hpp>

using namespace graphlab;

constexpr size_t NUM_SEGMENTS = 1;

int main(int argc, char **argv) {

  if(argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <output directory> <type-hints> " <<  std::endl;
    return -1;
  }
  
  //TODO: Figure out the temp file situation.

  // Set up tokenizer options
  csv_line_tokenizer tokenizer;

  if(argc == 3) { // only use the comma-separated tokenizer if it is a schemaRDD 
    tokenizer.delimiter = ',';
  } else {
    tokenizer.delimiter = '\n';
  }

  tokenizer.comment_char = '\0';
  tokenizer.escape_char = '\\';
  tokenizer.double_quote = true;
  tokenizer.quote_char = '\"';
  tokenizer.skip_initial_space = true;
  tokenizer.na_values.clear();
  tokenizer.init();
  
  std::vector<std::string> column_names;
  std::string first_line;
  std::vector<std::string> first_line_tokens;
  
  std::istream& fin = std::cin;
  
  if (!fin.good()) {
    log_and_throw("Fail reading from standard input");
  }

    
  if(!std::getline(fin,first_line)) { 
    return 0;
  }
  boost::algorithm::trim(first_line);
  tokenizer.tokenize_line(first_line.c_str(), 
                          first_line.length(), 
                          first_line_tokens);

  size_t ncols = first_line_tokens.size();
  if (ncols == 0) throw(std::string("No data received from input pipe!"));

  column_names.resize(ncols);
  for (size_t i = 0;i < ncols; ++i) {
    column_names[i] = "X" + std::to_string(i + 1);
  }
  
  // do not clear first_line and first_line_tokens. They are actual data.
  std::vector<flex_type_enum> column_types = std::vector<flex_type_enum>(ncols, flex_type_enum::STRING);
  std::vector<flexible_type> tokens = std::vector<flexible_type>(ncols,flex_string());

  
  std::shared_ptr<flexible_type_parser> parser(new flexible_type_parser(tokenizer.delimiter,tokenizer.escape_char)); 
  
  if(argc == 3) { 
    std::string type_hints(argv[2]);
     
    boost::char_separator<char> sep(",");
    boost::tokenizer< boost::char_separator<char> > tok(type_hints, sep);
    boost::tokenizer< boost::char_separator<char> >::iterator beg = tok.begin();
    size_t index = 0;
    while(beg != tok.end()) { 
      std::string _type = (*beg);
      if(_type == "int") { 
        column_types[index] = flex_type_enum::INTEGER;
        if (tokens[index].get_type() != flex_type_enum::INTEGER)
          tokens[index].reset(flex_type_enum::INTEGER);
      } 
      else if(_type == "float") { 
        column_types[index] = flex_type_enum::FLOAT;
        if (tokens[index].get_type() != flex_type_enum::FLOAT)
          tokens[index].reset(flex_type_enum::FLOAT);
      } 
      else if(_type == "str" || _type == "unicode") { 
        column_types[index] = flex_type_enum::STRING;
        if (tokens[index].get_type() != flex_type_enum::STRING)
          tokens[index].reset(flex_type_enum::STRING);
      } 
      else { 
        log_and_throw("Only basic types int,float,str are supported at this time."); 
      }  
      index++;  
      beg++;
    }  
    if(index != ncols) { 
      log_and_throw("number of type_hints is not equal to number of actual columns"); 
    } 
  } else {
    // first_line is already trimmed
    const char* first_line_ptr = first_line.c_str();
    std::pair<flexible_type, bool> ret = 
         parser->general_flexible_type_parse(&(first_line_ptr),first_line.size());
    if(ret.second && first_line_ptr == first_line.c_str() + first_line.size()) { 
      // We know that ncols is one
      column_types[0] = ret.first.get_type();
      tokens[0].reset(ret.first.get_type());
    } 
  }  
  // Create the SFrame to write to
  //TODO: Repeat this until done
  boost::uuids::uuid file_prefix = boost::uuids::random_generator()();

  std::stringstream ss;
  // TODO: Join paths correctly
  ss << argv[1] << "/" << file_prefix << ".frame_idx";
  sframe frame;
  frame.open_for_write(column_names, column_types, "", 1, false);
  
  std::string output="";
  size_t output_len = 0; 
  for(size_t sidx = 0; sidx < NUM_SEGMENTS; ++sidx) {
      auto it_out = frame.get_output_iterator(sidx);
      
      // Read the first line that was used for probing ... it only works here because we know num_segments = 1
      size_t _num_col = tokenizer.tokenize_line(first_line.c_str(),first_line.size(),tokens,true);
      for(size_t i=0;i<tokens.size();i++) { 
        if(tokens[i].get_type() == flex_type_enum::STRING) { 
          comma_unescape_string(tokens[i].get<flex_string>(),output,output_len);
          output.resize(output_len);
          tokens[i].mutable_get<flex_string>() = output;
        }
      }
      *it_out = tokens;
      ++it_out; 
      
      for (std::string curVal; std::getline(fin, curVal); ) {    
           _num_col = tokenizer.tokenize_line(curVal.c_str(),curVal.size(),
                       tokens,
                       true);
          
          if(_num_col != ncols){
            log_and_throw("number of tokens in parsed column does not match with the sframe number of columns");
          }
          
          for(size_t i=0;i<tokens.size();i++) { 
            if(tokens[i].get_type() == flex_type_enum::STRING) { 
              comma_unescape_string(tokens[i].get<flex_string>(),output,output_len);
              output.resize(output_len);
              tokens[i].mutable_get<flex_string>() = output;
            }
          }
          *it_out = tokens;
          ++it_out;
      }
  }
      
  if(frame.is_opened_for_write()) {
    frame.close();
  }
  std::string index_str = ss.str();
  frame.save(index_str);

  // Only send index path if save succeeded
  std::cout << index_str << std::endl;

  //fin.close();
  return 0;
}
