$(function () {
    console.log('ready');
    $('#datetimepicker6').datetimepicker();
    $('#datetimepicker7').datetimepicker({
       useCurrent: false //Important! See issue #1075
    });
    $("#datetimepicker6").on("dp.change", function (e) {
       $('#datetimepicker7').data("DateTimePicker").minDate(e.date);
    });
    $("#datetimepicker7").on("dp.change", function (e) {
       $('#datetimepicker6').data("DateTimePicker").maxDate(e.date);
    });

    $('#datetimepicker-submit').on('click', function(e) {
        var from = $('#datetimepicker6 input').val(),
           to =   $('#datetimepicker7 input').val();


        var fromDate = Date.parse(from);
        var toDate = Date.parse(to);

        window.location.href ="/between/"+fromDate+"/"+toDate;

    })
});