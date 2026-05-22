(function () {
  function getCellValue(row, idx) {
    var cell = row.children[idx];
    if (!cell) return "";
    var v = cell.getAttribute("data-value");
    return v !== null ? v : cell.textContent.trim();
  }
  function comparator(idx, type, asc) {
    return function (a, b) {
      var av = getCellValue(a, idx);
      var bv = getCellValue(b, idx);
      var r;
      if (type === "number") {
        var an = parseFloat(av);
        var bn = parseFloat(bv);
        if (isNaN(an)) an = -Infinity;
        if (isNaN(bn)) bn = -Infinity;
        r = an - bn;
      } else {
        r = av.localeCompare(bv);
      }
      return asc ? r : -r;
    };
  }
  function sortTable(table, idx, type) {
    var tbody = table.tBodies[0];
    if (!tbody) return;
    var allRows = Array.prototype.slice.call(tbody.rows);
    var topicRows = [];
    var detailFor = {};
    for (var i = 0; i < allRows.length; i++) {
      var r = allRows[i];
      if (r.classList.contains("topic-row")) {
        topicRows.push(r);
      } else if (r.classList.contains("topic-detail")) {
        var prev = topicRows[topicRows.length - 1];
        if (prev) {
          detailFor[prev.getAttribute("data-topic")] = r;
        }
      }
    }
    var asc = table.getAttribute("data-sort-col") === String(idx) ? table.getAttribute("data-sort-asc") !== "true" : true;
    topicRows.sort(comparator(idx, type, asc));
    table.setAttribute("data-sort-col", String(idx));
    table.setAttribute("data-sort-asc", asc ? "true" : "false");
    for (var j = 0; j < topicRows.length; j++) {
      tbody.appendChild(topicRows[j]);
      var det = detailFor[topicRows[j].getAttribute("data-topic")];
      if (det) tbody.appendChild(det);
    }
  }
  function init() {
    var table = document.getElementById("topics-table");
    if (!table) return;
    var ths = table.tHead.rows[0].cells;
    for (var i = 0; i < ths.length; i++) {
      (function (idx, th) {
        th.addEventListener("click", function () {
          sortTable(table, idx, th.getAttribute("data-sort") || "string");
        });
      })(i, ths[i]);
    }
    var rows = table.querySelectorAll(".topic-row");
    for (var k = 0; k < rows.length; k++) {
      rows[k].addEventListener("click", function () {
        var topic = this.getAttribute("data-topic");
        var nxt = this.nextElementSibling;
        if (nxt && nxt.classList.contains("topic-detail")) {
          nxt.hidden = !nxt.hidden;
        }
      });
    }
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
