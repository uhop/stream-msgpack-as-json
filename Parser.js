'use strict';

const {Transform} = require('stream');

class Parser extends Transform {
  static make(options) {
    return new Parser(options);
  }

  constructor(options) {
    super(Object.assign({}, options, {writableObjectMode: false, readableObjectMode: true}));

    this._packKeys = this._packStrings = this._packNumbers = this._streamKeys = this._streamStrings = this._streamNumbers = true;
    if (options) {
      'packValues' in options && (this._packKeys = this._packStrings = this._packNumbers = options.packValues);
      'packKeys' in options && (this._packKeys = options.packKeys);
      'packStrings' in options && (this._packStrings = options.packStrings);
      'packNumbers' in options && (this._packNumbers = options.packNumbers);
      'streamValues' in options && (this._streamKeys = this._streamStrings = this._streamNumbers = options.streamValues);
      'streamKeys' in options && (this._streamKeys = options.streamKeys);
      'streamStrings' in options && (this._streamStrings = options.streamStrings);
      'streamNumbers' in options && (this._streamNumbers = options.streamNumbers);
      this._jsonStreaming = options.jsonStreaming;
    }
    !this._packKeys && (this._streamKeys = true);
    !this._packStrings && (this._streamStrings = true);
    !this._packNumbers && (this._streamNumbers = true);

    this._buffer = null;
    this._done = false;
    this._expect = 'value';
    this._counter = 0;
    this._stack = [];
    this._encodeKey = false;
    this._accumulator = '';
  }

  _transform(chunk, encoding, callback) {
    this._buffer = this._buffer ? Buffer.concat(this._buffer, chunk) : chunk;
    this._processInput(callback);
  }

  _flush(callback) {
    this._done = true;
    this._processInput(callback);
  }

  _processInput(callback) {
    let index = 0;
    main: while (index < this._buffer.length) {
      // special cases
      switch (this._expect) {
        case 'binary':
          if (index + this._counter > this._buffer.length) {
            if (this._done) return callback(new Error('Cannot read bytes: no more input'));
            this._counter -= this._buffer.length - index;
            const v = this._buffer.toString('binary', index);
            if (this._encodeKey) {
              this._streamKeys && this.push({name: 'stringChunk', value: v});
            } else {
              this._streamStrings && this.push({name: 'stringChunk', value: v});
            }
            this._packStrings && (this._accumulator += v);
            index = this._buffer.length;
            break main; // wait for more input
          }
          const v = this._buffer.toString('binary', index, index + this._counter);
          index += this._counter;
          this._counter = 0;
          if (this._encodeKey) {
            if (this._streamKeys) {
              this.push({name: 'stringChunk', value: v});
              this.push({name: 'endKey'});
            }
            this._packKeys && this.push({name: 'keyValue', value: this._accumulator + v});
          } else {
            if (this._streamStrings) {
              this.push({name: 'stringChunk', value: v});
              this.push({name: 'endString'});
            }
            this._packStrings && this.push({name: 'stringValue', value: this._accumulator + v});
          }
          this._accumulator = '';
          this._expected = 'value';
          continue;
        case 'skip':
          if (index + this._counter > this._buffer.length) {
            if (this._done) return callback(new Error('Cannot skip bytes: no more input'));
            this._counter -= this._buffer.length - index;
            index = this._buffer.length;
            break main; // wait for more input
          }
          index += this._counter;
          this._counter = 0;
          this._expected = 'value';
          continue;
      }

      // close objects
      while (this._stack.length && !this._stack[this._stack.length - 1]) {
        this._stack.pop();
        this.push({name: this._stack.pop() ? 'endArray' : 'endObject'});
      }
      this._encodeKey = this._stack.length && !this._stack[this._stack.length - 2] && !(this._stack[this._stack.length - 1] & 1);
      this._stack.length && --this._stack[this._stack.length - 1];

      // start interpreting commands
      const value = this._buffer[index++];
      if (!(value & 0x80)) {
        // positive fixint => number
        const v = value.toString();
        if (this._streamNumbers) {
          this.push({name: 'startNumber'});
          this.push({name: 'numberChunk', value: v});
          this.push({name: 'endNumber'});
        }
        this._packNumbers && this.push({name: 'numberValue', value: v});
        continue;
      }
      switch (value & 0xf0) {
        case 0x80: // fixmap
          this.push({name: 'startObject'});
          this._stack.push(false, (value & 0x0f) << 1);
          continue;
        case 0x90: // fixarray
          this.push({name: 'startArray'});
          this._stack.push(true, value & 0x0f);
          continue;
        case 0xa0: // fixstr
        case 0xb0:
          const length = value & 0x1f;
          if (index + length > this._buffer.length) {
            if (this._done) return callback(new Error('Cannot parse fixstr: no more input'));
            break main; // wait for more input
          }
          const v = this._buffer.toString('utf8', index, index + length);
          if (this._encodeKey) {
            if (this._streamKeys) {
              this.push({name: 'startKey'});
              this.push({name: 'stringChunk', value: v});
              this.push({name: 'endKey'});
            }
            this._packKeys && this.push({name: 'keyValue', value: v});
          } else {
            if (this._streamStrings) {
              this.push({name: 'startString'});
              this.push({name: 'stringChunk', value: v});
              this.push({name: 'endString'});
            }
            this._packStrings && this.push({name: 'stringValue', value: v});
          }
          index += length;
          continue;
        case 0xe0: // negative fixint
        case 0xf0:
          const v = (value - 256).toString();
          if (this._streamNumbers) {
            this.push({name: 'startNumber'});
            this.push({name: 'numberChunk', value: v});
            this.push({name: 'endNumber'});
          }
          this._packNumbers && this.push({name: 'numberValue', value: v});
          continue;
      }
      switch (value) {
        case 0xc0: // nil
          this.push({name: 'nullValue', value: null});
          continue;
        case 0xc2: // false
          this.push({name: 'falseValue', value: false});
          continue;
        case 0xc3: // true
          this.push({name: 'trueValue', value: true});
          continue;
        case 0xca: // float 32
          if (index + 4 > this._buffer.length) {
            if (this._done) return callback(new Error('Cannot parse float 32: no more input'));
            break main; // wait for more input
          }
          const v = this._buffer.readFloatBE(index).toString();
          if (this._streamNumbers) {
            this.push({name: 'startNumber'});
            this.push({name: 'numberChunk', value: v});
            this.push({name: 'endNumber'});
          }
          this._packNumbers && this.push({name: 'numberValue', value: v});
          index += 4;
          continue;
        case 0xcb: // float 64
          if (index + 8 > this._buffer.length) {
            if (this._done) return callback(new Error('Cannot parse float 64: no more input'));
            break main; // wait for more input
          }
          const v = this._buffer.readDoubleBE(index).toString();
          if (this._streamNumbers) {
            this.push({name: 'startNumber'});
            this.push({name: 'numberChunk', value: v});
            this.push({name: 'endNumber'});
          }
          this._packNumbers && this.push({name: 'numberValue', value: v});
          index += 8;
          continue;
        case 0xcf: // uint 64
          if (index + 8 > this._buffer.length) {
            if (this._done) return callback(new Error('Cannot parse uint 64: no more input'));
            break main; // wait for more input
          }
          const v = (this._buffer.readUIntBE(index, 6) * 0x10000 + this._buffer.readUInt16BE(index + 6)).toString();
          if (this._streamNumbers) {
            this.push({name: 'startNumber'});
            this.push({name: 'numberChunk', value: v});
            this.push({name: 'endNumber'});
          }
          this._packNumbers && this.push({name: 'numberValue', value: v});
          index += 8;
          continue;
        case 0xd3: // int 64
          if (index + 8 > this._buffer.length) {
            if (this._done) return callback(new Error('Cannot parse uint 64: no more input'));
            break main; // wait for more input
          }
          const hi = this._buffer.readIntBE(index, 6),
            lo = this._buffer.readUInt16BE(index + 6),
            v = hi < 0 ? -((~-hi + 1) & 0xffffffffffff) * 0x10000 - lo : (hi * 0x10000 + lo).toString();
          if (this._streamNumbers) {
            this.push({name: 'startNumber'});
            this.push({name: 'numberChunk', value: v});
            this.push({name: 'endNumber'});
          }
          this._packNumbers && this.push({name: 'numberValue', value: v});
          index += 8;
          continue;
        case 0xd4: // fixext 1
          this._counter = 2;
          this._expect = 'skip';
          continue;
        case 0xd5: // fixext 2
          this._counter = 3;
          this._expect = 'skip';
          continue;
        case 0xd6: // fixext 4
          this._counter = 5;
          this._expect = 'skip';
          continue;
        case 0xd7: // fixext 8
          this._counter = 9;
          this._expect = 'skip';
          continue;
        case 0xd8: // fixext 16
          this._counter = 17;
          this._expect = 'skip';
          continue;
        case 0xc4: // bin 8
        case 0xc7: // ext 8
        case 0xcc: // uint 8
        case 0xd0: // int 8
        case 0xd9: // str 8
          if (index >= this._buffer.length) {
            if (this._done) return callback(new Error('Cannot parse 8-bit value: no more input'));
            break main; // wait for more input
          }
          const n = this._buffer[index++].toString();
          if (value === 0xc4) {
            if (this._encodeKey) {
              this._streamKeys && this.push({name: 'startKey'});
            } else {
              this._streamStrings && this.push({name: 'startString'});
            }
            this._counter = n;
            this._expect = 'binary';
            continue;
          }
          if (value === 0xc7) {
            this._counter = n + 1;
            this._expect = 'skip';
            continue;
          }
          if (value === 0xcc || value === 0xd0) {
            const v = (value === 0xcc || n < 128 ? n : n - 256).toString();
            if (this._streamNumbers) {
              this.push({name: 'startNumber'});
              this.push({name: 'numberChunk', value: v});
              this.push({name: 'endNumber'});
            }
            this._packNumbers && this.push({name: 'numberValue', value: v});
            continue;
          }
          if (index + n > this._buffer.length) {
            if (this._done) return callback(new Error('Cannot parse bin/str 8: no more input'));
            break main; // wait for more input
          }
          const v = this._buffer.toString('utf8', index, index + n);
          if (this._encodeKey) {
            if (this._streamKeys) {
              this.push({name: 'startKey'});
              this.push({name: 'stringChunk', value: v});
              this.push({name: 'endKey'});
            }
            this._packKeys && this.push({name: 'keyValue', value: v});
          } else {
            if (this._streamStrings) {
              this.push({name: 'startString'});
              this.push({name: 'stringChunk', value: v});
              this.push({name: 'endString'});
            }
            this._packStrings && this.push({name: 'stringValue', value: v});
          }
          index += n;
          continue;
        case 0xc5: // bin 16
        case 0xc8: // ext 16
        case 0xcd: // uint 16
        case 0xd1: // int 16
        case 0xda: // str 16
        case 0xdc: // array 16
        case 0xde: // map 16
          if (index + 2 > this._buffer.length) {
            if (this._done) return callback(new Error('Cannot parse 16-bit value: no more input'));
            break main; // wait for more input
          }
          const n = this._buffer[value === 0xd1 ? 'readInt16BE' : 'readUInt16BE'](index).toString();
          index += 2;
          if (value === 0xc5) {
            if (this._encodeKey) {
              this._streamKeys && this.push({name: 'startKey'});
            } else {
              this._streamStrings && this.push({name: 'startString'});
            }
            this._counter = n;
            this._expect = 'binary';
            continue;
          }
          if (value === 0xc8) {
            this._counter = n + 1;
            this._expect = 'skip';
            continue;
          }
          if (value === 0xcd || value === 0xd1) {
            const v = n.toString();
            if (this._streamNumbers) {
              this.push({name: 'startNumber'});
              this.push({name: 'numberChunk', value: v});
              this.push({name: 'endNumber'});
            }
            this._packNumbers && this.push({name: 'numberValue', value: v});
            continue;
          }
          if (value === 0xdc) {
            this.push({name: 'startArray'});
            this._stack.push(true, n);
            continue;
          }
          if (value === 0xde) {
            this.push({name: 'startObject'});
            this._stack.push(false, n << 1);
            continue;
          }
          if (index + n > this._buffer.length) {
            if (this._done) return callback(new Error('Cannot parse bin/str 16: no more input'));
            break main; // wait for more input
          }
          const v = this._buffer.toString('utf8', index, index + n);
          if (this._encodeKey) {
            if (this._streamKeys) {
              this.push({name: 'startKey'});
              this.push({name: 'stringChunk', value: v});
              this.push({name: 'endKey'});
            }
            this._packKeys && this.push({name: 'keyValue', value: v});
          } else {
            if (this._streamStrings) {
              this.push({name: 'startString'});
              this.push({name: 'stringChunk', value: v});
              this.push({name: 'endString'});
            }
            this._packStrings && this.push({name: 'stringValue', value: v});
          }
          index += n;
          continue;
        case 0xc6: // bin 32
        case 0xc9: // ext 32
        case 0xce: // uint 32
        case 0xd2: // int 32
        case 0xdb: // str 32
        case 0xdd: // array 32
        case 0xdf: // map 32
          if (index + 4 > this._buffer.length) {
            if (this._done) return callback(new Error('Cannot parse 32-bit: no more input'));
            break main; // wait for more input
          }
          const n = this._buffer[value === 0xd2 ? 'readInt32BE' : 'readUInt32BE'](index).toString();
          index += 4;
          if (value === 0xc6) {
            if (this._encodeKey) {
              this._streamKeys && this.push({name: 'startKey'});
            } else {
              this._streamStrings && this.push({name: 'startString'});
            }
            this._counter = n;
            this._expect = 'binary';
            continue;
          }
          if (value === 0xc9) {
            this._counter = n + 1;
            this._expect = 'skip';
            continue;
          }
          if (value === 0xce || value === 0xd2) {
            const v = n.toString();
            if (this._streamNumbers) {
              this.push({name: 'startNumber'});
              this.push({name: 'numberChunk', value: v});
              this.push({name: 'endNumber'});
            }
            this._packNumbers && this.push({name: 'numberValue', value: v});
            continue;
          }
          if (value === 0xdd) {
            this.push({name: 'startArray'});
            this._stack.push(true, n);
            continue;
          }
          if (value === 0xdf) {
            this.push({name: 'startObject'});
            this._stack.push(false, n << 1);
            continue;
          }
          if (index + n > this._buffer.length) {
            if (this._done) return callback(new Error('Cannot parse bin/str 32: no more input'));
            break main; // wait for more input
          }
          const v = this._buffer.toString('utf8', index, index + n);
          if (this._encodeKey) {
            if (this._streamKeys) {
              this.push({name: 'startKey'});
              this.push({name: 'stringChunk', value: v});
              this.push({name: 'endKey'});
            }
            this._packKeys && this.push({name: 'keyValue', value: v});
          } else {
            if (this._streamStrings) {
              this.push({name: 'startString'});
              this.push({name: 'stringChunk', value: v});
              this.push({name: 'endString'});
            }
            this._packStrings && this.push({name: 'stringValue', value: v});
          }
          index += n;
          continue;
      }
    }

    // clean up
    if (index < this._buffer.length) {
      this._buffer = this._buffer.slice(index);
    } else {
      while (this._stack.length && !this._stack[this._stack.length - 1]) {
        this._stack.pop();
        this.push({name: this._stack.pop() ? 'endArray' : 'endObject'});
      }
      this._buffer = null;
    }

    // final checks
    if (this._done && (this._buffer || this._stack.length)) return callback(new Error('Unfinished parsing: no more input'));

    callback(null);
  }
}
Parser.parser = Parser.make;
Parser.make.Constructor = Parser;

module.exports = Parser;
