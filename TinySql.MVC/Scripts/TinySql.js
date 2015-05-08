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


    
    $('table tbody').on('click', 'tr', function () {
        PostBack(table.row(this).data());
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

    function PostBack(d) {
        $.ajax({
            url: "/TinySql/Update",
            data: { rowData: JSON.stringify(d) },
            dataType: "text",
            method: "POST",
            cache: false,
            contentType: 'application/x-www-form-urlencoded; charset=UTF-8',
            //accepts: "application/json",
            success: function (data,jqXhr) {
                if (data != null) {
                    alert(data);
                    window.location.reload(true);
                }
                else {
                    alert("hmm?");
                }
            }
        }).done(function (data) {
            
            
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
