import 'dart:math' as Math;

class CommandLineTable{
  final HORIZONTAL_SEP = "-";
  String verticalSep = "|";
  String joinSep = "+";
  List<String> headers;
  List<List> rows = [];
  bool rightAlign = false;

  CommandLineTable() {
      setShowVerticalLines(false);
  }

  void setRightAlign(bool rightAlign) {
      this.rightAlign = rightAlign;
  }

  void setShowVerticalLines(bool showVerticalLines) {
      verticalSep = showVerticalLines ? "|" : "";
      joinSep = showVerticalLines ? "+" : " ";
  }

  void setHeaders(List<String> headers) {
      this.headers = headers;
  }

  void addRow(List cells) {
      rows.add(cells);
  }

  void doPrint() {
      List<int> maxWidths = headers?.map((text) => text.length)?.toList();

      for (List cells in rows) {
          if (maxWidths == null) {
              maxWidths = List.filled(cells.length, 0);
          }
          if (cells.length != maxWidths.length) {
              throw new Exception("Number of row-cells and headers should be consistent");
          }
          for (int i = 0; i < cells.length; i++) {
              maxWidths[i] = Math.max(maxWidths[i], cells[i].toString().length);
          }
      }

      if (headers != null) {
          printLine(maxWidths);
          printRow(headers, maxWidths);
          printLine(maxWidths);
      }
      for (List cells in rows) {
          printRow(cells, maxWidths);
      }
      if (headers != null) {
          printLine(maxWidths);
      }
  }

  void printLine(List<int> columnWidths) {
    String batch = "";
    for (int i = 0; i < columnWidths.length; i++) {
      String line = List.generate(columnWidths[i] +
                verticalSep.length + 1, (_) => HORIZONTAL_SEP).join("");
      batch += (joinSep + line + (i == columnWidths.length - 1 ? joinSep : ""));
    }
    print(batch);
  }

  void printRow(List cells, List<int> maxWidths) {
    String batch = "";
    for (int i = 0; i < cells.length; i++) {
        String s = cells[i].toString();
        String verStrTemp = i == cells.length - 1 ? verticalSep : "";
        if (rightAlign) {
          batch += verticalSep + " " + s.padRight(maxWidths[i]) + " " + verStrTemp;
          //System.out.printf("%s %" + maxWidths[i] + "s %s", verticalSep, s, verStrTemp);
        } 
        else {
          batch += verticalSep + " " + s.padLeft(maxWidths[i]) + " " + verStrTemp;
          //System.out.printf("%s %-" + maxWidths[i] + "s %s", verticalSep, s, verStrTemp);
        }
    }
    print(batch);
  }
}