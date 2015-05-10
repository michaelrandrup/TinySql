$(document).ready(function () {
    // var table = $('table').DataTable();

    var aoColumns = [];

    $('table thead th').each(function () {
        // var sType = $(this).getAttribute("data-name");
        var name = $(this).data("name");
        var type = $(this).data("type");
        aoColumns.push(name ? { "data": name, "type": type } : null);
    });



    var table = $('table').DataTable({
        "ajax": '/TinySql/Contacts',
        //"columns": [
        //    { "data": "ContactID" },
        //    { "data": "Name" },
        //    { "data": "Telephone" },
        //    { "data": "WorkEmail" }
        //],
        "sAjaxDataProp": "",
        // "columns": $('table').find('thead tr th').map(function(){return $(this).data()})
        "columns": aoColumns,
        "stateSave": true
    });


    $('table tbody').on('dblclick', 'tr', function () {
        PostBack(table.cells(this,0).data()[0]);
    });

    
    $('table tbody').on('click', 'tr', function () {
        console.log(table.row(this).data());
        console.log(table.cell(table.row(this).toJQuery(), 0).data());
        if ($(this).hasClass('selected')) {
            $(this).removeClass('selected');
            
        }
        else {
            table.$('tr.selected').removeClass('selected');
            $(this).addClass('selected');
        }
    });

    function PostBack(cell) {
        // var d = row.data();
        $.ajax({
            url: "/TinySql/Edit",
            data: {Id: cell },
            //dataType: "json",
            method: "POST",
            cache: false,
            //accepts: "text/html",
            //contentType: 'application/x-www-form-urlencoded; charset=UTF-8',
            success: function (data, jqXhr) {
                if (data != null) {
                    $("#dialog").remove();
                    $("#tinysql").append(data);
                    $("#dialog").modal('show');
                    //row.data(JSON.parse(data)).draw(false);
                }
                else {
                    alert("hmm?");
                }
            }
        }).done(function (data) {
        //$.ajax({
        //    url: "/TinySql/Edit",
        //    data: { rowData: JSON.stringify(d) },
        //    dataType: "text",
        //    method: "POST",
        //    cache: false,
        //    accepts: "application/json",
        //    contentType: 'application/x-www-form-urlencoded; charset=UTF-8',
        //    success: function (data,jqXhr) {
        //        if (data != null) {
        //            row.data(JSON.parse(data)).draw(false);
        //        }
        //        else {
        //            alert("hmm?");
        //        }
        //    }
        //}).done(function (data) {
            
            
        }).fail(function (data) {
            alert(data);
        })

        //$.post("/TinySql/Update", { rowData: JSON.stringify(d) }, function () {
        //    alert("OK");
        //});
    }

    //$('#button').click(function () {
    //    table.row('.selected').remove().draw(false);
    //});
});
