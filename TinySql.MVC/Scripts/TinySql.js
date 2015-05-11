function LoadList(tableName, listType, listName, container) {

    //
    // Load table definition into container
    //
    $.get("/TinySql/List", { TableName: tableName, ListType: listType, ListName: listName }, function (data, textStatus, jqXHR) {
        $(container).html(data);

    }).error(function (data, textStatus, jqXHR) {
        console.log(data);
        console.log(textStatus);
        console.log(jqXHR);
    }).always(function () {

    }).done(function (data) {

        //
        // Initialize table and load data
        //
        var aoColumns = [];
        $('table thead th', container).each(function () {
            // var sType = $(this).getAttribute("data-name");
            var name = $(this).data("name");
            var type = $(this).data("type");
            aoColumns.push(name ? { "data": name, "type": type } : null);
        });

        var table = $('table', container).DataTable({
            "ajax": '/TinySql/Rows?TableName=' + tableName + '&ListType=' + listType + '&ListName=' + listName,
            "sAjaxDataProp": "",
            "columns": aoColumns,
            "stateSave": true
        });


        //
        // Set selected row event
        //
        $('table tbody', container).on('click', 'tr', function () {
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

        //
        // Set Edit item event
        //
        $('table tbody', container).on('dblclick', 'tr', function () {

            var cell = table.cells(this, 0).data()[0];
            var rowIndex = table.row(this).index();
            var tableName = $('table',container).data("table");
            var listType = $('table',container).data("listtype");
            var listName = $('table',container).data("listname");

            $.ajax({
                url: "/TinySql/Edit",
                data: {
                    Id: cell,
                    Table: tableName,
                    ListType: listType,
                    ListName: listName
                },
                method: "POST",
                cache: false,
                success: function (data, jqXhr) {
                    if (data != null) {
                        $("#dialog").remove();
                        $("#tinysql").append(data);
                        $("#dialog").modal('show');
                        $("#dialog").on("hidden.bs.modal", function (event) {
                            $("#dialog").remove();
                        });

                        $('.btnsave', '#tinysql').on("click", function (event) {
                            event.preventDefault();

                            $.post("/TinySql/Save", $('form', '#tinysql').serialize(), function (data, textStatus, jqXHR) {
                                table.row(rowIndex).data(data).draw(false);
                                $("#dialog").modal('hide');
                            }).error(function (data, textStatus, jqXHR) {
                                console.log(data);
                                console.log(textStatus);
                                console.log(jqXHR);
                            }).always(function () {
                                
                            });

                        });
                    }
                    else {
                        alert("hmm?");
                    }
                }
            }).done(function (data) {

            }).fail(function (data) {
                alert(data);
            })
        });



    });


}

