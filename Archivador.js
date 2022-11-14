/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/log', 'N/record', 'N/search','N/runtime','N/file'],

    (log, record, search, runtime,file) => {


        const getInputData = (inputContext) => {
            try{
                var arrayObj = {};
                var arrayMain = [];
                var mySearch = search.load({
                    id: 'customsearch_tkio_archivador'
                    //id: 'customsearch1397'
                });
                var myPagedResults = mySearch.runPaged({
                    pageSize: 1000
                });
                var thePageRanges = myPagedResults.pageRanges;
                for (var i in thePageRanges) {

                    var thepageData = myPagedResults.fetch({
                        index: thePageRanges[i].index
                    });

                    var indexx = thePageRanges[i].index;
                    thepageData.data.forEach(function (result, indexx ) {
                        var typeTrans = result.getText({name: 'type' , summary: "GROUP"});
                        var IDDoc = result.getText({name: 'internalid', join: 'file' , summary: "GROUP" });
                        var subsidiaria = result.getText({name: 'subsidiary' , summary: "GROUP" });
                        var Empleado = result.getValue({name: 'altname', join: 'employee'  , summary: "GROUP"});
                        var LVLempleado = result.getText({name: 'custentity_efx_employee_nivel', join: 'employee'  , summary: "GROUP"});
                        var typeDoc = result.getValue({name: 'filetype', join: 'filetype' , summary: "GROUP"});
                        var fecha = result.getValue({name: 'trandate' , summary: "GROUP"});
                        var id = result.getText({name: 'internalid', join: 'file' , summary: "GROUP" });

                        arrayMain.push({id:id, typeTrans: typeTrans, IDDoc: IDDoc, subsidiaria:subsidiaria,/*PDF:PDF,XML:XML,*/Empleado:Empleado,LVLempleado:LVLempleado,typeDoc:typeDoc,fecha:fecha});

                        return true;
                    })

                }
                var customrecord_ncfar_assetSearchObj = search.create({
                    type: "customrecord_ncfar_asset",
                    filters:
                        [
                            ["file.name","isnotempty",""]
                        ],
                    columns:
                        [
                            search.createColumn({name: "custrecord_assetsubsidiary", label: "Subsidiaria"}),
                            search.createColumn({
                                name: "internalid",
                                join: "file",
                                label: "ID interno"
                            }),
                            search.createColumn({
                                name: "name",
                                join: "file",
                                label: "Nombre"
                            }),
                            search.createColumn({name: "created", label: "Fecha de creación"})
                        ]
                });

                var searchResultCount = customrecord_ncfar_assetSearchObj.runPaged().count;
                customrecord_ncfar_assetSearchObj.run().each(function(result){
                    var typeTrans2 = 'Creación Activo';
                    var IDDoc2 = result.getValue({name: 'internalid', join: 'file'});
                    var subsidiaria2 = result.getText({name: "custrecord_assetsubsidiary" });
                    var fecha2 = result.getValue({name: 'created' });

                    arrayMain.push({id:IDDoc2, typeTrans: typeTrans2, IDDoc: IDDoc2, subsidiaria:subsidiaria2,fecha:fecha2});

                    return true;
                });

                var vendorprepaymentSearchObj = search.create({
                    type: "vendorprepayment",
                    filters:
                        [
                            ["type","anyof","VPrep"],
                            "AND",
                            ["file.internalidnumber","greaterthan","0"]
                        ],
                    columns:
                        [
                            search.createColumn({name: "subsidiary", label: "Subsidiaria"}),
                            search.createColumn({
                                name: "internalid",
                                join: "file",
                                label: "ID interno"
                            }),
                            search.createColumn({
                                name: "name",
                                join: "file",
                                label: "Nombre"
                            }),
                            search.createColumn({name: "trandate", label: "Fecha"})
                        ]
                });

                vendorprepaymentSearchObj.run().each(function(result){
                    var typeTrans3 = 'Pago anticipado';
                    var IDDoc3 = result.getValue({name: 'internalid', join: 'file'});
                    var subsidiaria3 = result.getText({name: "subsidiary" });
                    var fecha3 = result.getValue({name: 'trandate' });
                    arrayMain.push({id:IDDoc3, typeTrans: typeTrans3, IDDoc: IDDoc3, subsidiaria:subsidiaria3,fecha:fecha3});

                    return true;
                });

                return arrayMain;
            }catch (e) {
                log.audit({title: 'Error in get input data', details: e });
            }

        }

        var index=0;
        const map = (mapContext) => {


            try {
                var datos = JSON.parse(mapContext.value);
                var peticion = datos.id;
                mapContext.write({
                    key: index,
                    value: datos
                });
                index++;
            } catch (e) {
                log.error({title: 'map - error', details: e});
            }

        }

        const reduce = (reduceContext) => {
            try {
                const fecha = new Date();
                var meses = ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'];
                var dias = ['Domingo','Lunes','Martes','Miercoles','Jueves','Viernes','Sabado']
                var mesnow = fecha.getMonth()+1;
                var yearnow = fecha.getFullYear();
                var daynow = fecha.getDate();
                var dayweeknow = fecha.getDay();
                var arrayCarpetas = [];
                var arrayCarpetasID = [];

                var data = JSON.parse(reduceContext.values[0]);
                var control = true;



                var busqueda_carpetas = search.create({
                    type: search.Type.FOLDER,
                    filters: [
                        ['parent', search.Operator.IS, '68989'],
                    ],
                    columns: [
                        search.createColumn({name: 'internalid'}),
                        search.createColumn({name: 'name'}),
                        search.createColumn({name: 'parent'}),
                    ]
                });

                busqueda_carpetas.run().each(function(result){
                    arrayCarpetas.push(result.getValue({name: 'name'}));
                    arrayCarpetasID.push(result.getValue({name: 'internalid'}))
                    return true;
                });
                if(data.typeTrans){
                    var transaccion = data.typeTrans;
                    if(transaccion == 'Nota de crédito'){
                        transaccion = 'Crédito al Proveedor';
                    }
                    if(transaccion == 'Pago de factura'){
                        transaccion = 'Pago factura Proveedor';
                    }
                    if(transaccion == 'Recibo de artículo'){
                        transaccion = 'Recibo';
                    }
                    if(transaccion == 'Registro factura cliente'){
                        transaccion = 'Factura';
                    }
                    if(transaccion == 'Pago'){
                        transaccion = 'Cobro factura cliente';
                    }
                    if(transaccion == 'Pago anticipado'){
                        transaccion = 'Anticipo Clientes';
                    }

                    if(transaccion.includes('Pago anticipado') ){
                        transaccion = 'Egreso';
                    }
                }

                if(data.LVLempleado){
                    var Empleadolvl =  data.LVLempleado;
                    if (Empleadolvl == 'DIRECTOR'){
                        var tipoEmpleado = 'Directivo';
                    } else {
                        var tipoEmpleado = 'General';
                    }

                }
                if(data.Empleado){
                    var nombreEmpleado = data.Empleado;
                }
                if(data.IDDoc){
                    var IDfile = data.IDDoc;
                }
                if(data.subsidiaria){
                    var Subsidiaria = data.subsidiaria;
                }
                if(data.fecha){
                    var date = data.fecha;
                }
                if(data.XML){
                    var IDxml = ''//data.XML;
                }
                if(data.PDF){
                    var IDpdf ='' //data.PDF;
                }
                if(data.IDfile){
                    var FileX = data.IDfile.value;
                }
                var arrayDate = date.split('/');
                var yeardate = parseInt(arrayDate[0]);
                var mounthdate = parseInt(arrayDate[1]);
                var daydate = parseInt(arrayDate[2]);
                var datecom = new Date(date);
                var diasem = datecom.getDay();
                var restrans = Transaccion(arrayCarpetas,arrayCarpetasID,transaccion);
                if(transaccion == 'Informe de gastos'){
                    var resTipo = tipoEmpleadofun(restrans,tipoEmpleado,transaccion)
                    var resSubsi = tipoSubsidiaria(resTipo,tipoEmpleado,Subsidiaria);
                    var resNombre = nombreEmpleadofun(resSubsi,nombreEmpleado,Subsidiaria)
                    var resyear = year(resNombre,yeardate,nombreEmpleado);
                    var resmes = mes(resyear,yeardate,mounthdate,meses);
                    var resdia = dia(resmes,dias,daydate,diasem,mounthdate,meses)
                    if(IDpdf){
                        archivador(IDpdf,resdia );
                    }
                    if(IDxml){
                        log.audit({title: 'IDxml', details: IDxml});
                        archivador(IDxml,resdia );
                    }
                    if(FileX){
                        archivador(FileX,resdia );
                    }
                    if(IDfile){
                        archivador(IDfile,resdia );
                    }
                }else{

                    var resSubsi = tipoSubsidiaria2(restrans,tipoEmpleado,transaccion,Subsidiaria);
                    var resyear = year2(resSubsi,yeardate,Subsidiaria);
                    var resmes = mes(resyear,yeardate,mounthdate,meses);
                    var resdia = dia(resmes,dias,daydate,diasem,mounthdate,meses)
                    if(IDpdf){
                        archivador(IDpdf,resdia );
                    }
                    if(IDxml){
                        archivador(IDxml,resdia );
                    }
                    if(FileX){
                        archivador(FileX,resdia );
                    }
                    if(IDfile){
                        archivador(IDfile,resdia );
                    }
                }


            }catch (e) {
                log.audit({title:'Error 329', details: e})
            }

        }

        function Transaccion(arrayCarpetas,arrayCarpetasID,transaccion) {
            try{
                var ObjetoCarpetas = new Object();
                var Carpeta = arrayCarpetas;
                var IDcarpeta = arrayCarpetasID;
                var control = true;
                if (Carpeta.length>0){
                    for(var i =0; i<Carpeta.length; i++){
                        if(Carpeta[i]==transaccion){
                            control = false;
                            break;
                        }
                    }
                    if(control == true){
                        var objRecord = record.create({
                            type: record.Type.FOLDER,
                            isDynamic: true
                        });

                        objRecord.setValue({
                            fieldId: 'name',
                            value: transaccion
                        });
                        objRecord.setValue({
                            fieldId: 'parent',
                            value: 68989
                        });

                        var folderId = objRecord.save({
                            enableSourcing: true,
                            ignoreMandatoryFields: true
                        })
                        Carpeta.push(transaccion);
                        IDcarpeta.push(folderId)
                    }
                } else  {
                    var objRecord = record.create({
                        type: record.Type.FOLDER,
                        isDynamic: true
                    });

                    objRecord.setValue({
                        fieldId: 'name',
                        value: transaccion
                    });
                    objRecord.setValue({
                        fieldId: 'parent',
                        value: 68989
                    });

                    var folderId = objRecord.save({
                        enableSourcing: true,
                        ignoreMandatoryFields: true
                    })

                    Carpeta.push(transaccion);
                    IDcarpeta.push(folderId)

                }


                ObjetoCarpetas.Datos = {
                    carpeta:  Carpeta,
                    carpetaID: IDcarpeta
                }

                return ObjetoCarpetas;
            }catch (e) {
                log.audit({title:'Error 407', details: e})
            }
        }
        function year(restrans,yearnow,transaccion) {
            try{
                var ObjetoCarpetas = new Object();
                var arrayYear = [];
                var arrayYearID = [];
                var arrayCarpetas = restrans.Datos.carpeta;
                var arrayCarpetasID= restrans.Datos.carpetaID;
                if(arrayCarpetasID.length>0) {
                    for (var j = 0; j < arrayCarpetasID.length; j++) {
                        if (transaccion == arrayCarpetas[j]) {
                            var busqueda_carpetas_in = search.create({
                                type: search.Type.FOLDER,
                                filters: [
                                    ['parent', search.Operator.IS, arrayCarpetasID[j]],
                                ],
                                columns: [
                                    search.createColumn({name: 'internalid'}),
                                    search.createColumn({name: 'name'}),
                                ]
                            });
                            var searchResultCount = busqueda_carpetas_in.runPaged().count;
                            if (searchResultCount < 1) {
                                var yearint = parseInt(yearnow);
                                var objRecord2 = record.create({
                                    type: record.Type.FOLDER,
                                    isDynamic: true
                                });

                                objRecord2.setValue({
                                    fieldId: 'name',
                                    value: yearint.toString()
                                });
                                objRecord2.setValue({
                                    fieldId: 'parent',
                                    value: arrayCarpetasID[j]
                                });

                                var folderId2 = objRecord2.save({
                                    enableSourcing: true,
                                    ignoreMandatoryFields: true
                                })
                                arrayYear.push(yearint.toString());
                                arrayYearID.push(folderId2)
                            } else {
                                busqueda_carpetas_in.run().each(function (result) {
                                    arrayYear.push(result.getValue({name: 'name'}));
                                    arrayYearID.push(result.getValue({name: 'internalid'}))
                                    return true;
                                });
                                var control2 = true;
                                for (var k = 0; k < arrayYear.length; k++) {
                                    if (arrayYear[k] == yearnow) {
                                        control2 = false;
                                        break;

                                    }
                                }
                                if (control2 == true) {

                                    var yearint3 = parseInt(yearnow);
                                    var objRecord3 = record.create({
                                        type: record.Type.FOLDER,
                                        isDynamic: true
                                    });

                                    objRecord3.setValue({
                                        fieldId: 'name',
                                        value: yearint3.toString()
                                    });
                                    objRecord3.setValue({
                                        fieldId: 'parent',
                                        value: arrayCarpetasID[j]
                                    });

                                    var folderId3 = objRecord3.save({
                                        enableSourcing: true,
                                        ignoreMandatoryFields: true
                                    })
                                }

                            }


                        }
                    }
                }
                ObjetoCarpetas.Datos = {
                    carpeta:  arrayYear,
                    carpetaID: arrayYearID
                }
                return ObjetoCarpetas;
            }catch (e) {
                log.audit({title:'Error 511', details: e})
            }
        }

        function year2(restrans,yearnow,Subsidiaria) {
            try{
                var ObjetoCarpetas = new Object();
                var arrayYear = [];
                var arrayYearID = [];
                var arrayCarpetas = restrans.Datos.carpeta;
                var arrayCarpetasID= restrans.Datos.carpetaID;


                if(arrayCarpetasID.length>0) {
                    for (var j = 0; j < arrayCarpetasID.length; j++) {
                        var x = arrayCarpetas[j];
                        var y = Subsidiaria;
                        if(x.length < 23){   var w = 'ini' }
                        else{
                            var w =x.substr(23,5)
                        }
                        if(y.length < 23){  var z = 'ini'  }
                        else{
                            var z =y.substr(23,5)
                        }

                        if (w==z) {

                            var busqueda_carpetas_in = search.create({
                                type: search.Type.FOLDER,
                                filters: [
                                    ['parent', search.Operator.IS, arrayCarpetasID[j]],
                                ],
                                columns: [
                                    search.createColumn({name: 'internalid'}),
                                    search.createColumn({name: 'name'}),
                                ]
                            });
                            var searchResultCount = busqueda_carpetas_in.runPaged().count;
                            if (searchResultCount < 1) {
                                var yearint = parseInt(yearnow);
                                var objRecord2 = record.create({
                                    type: record.Type.FOLDER,
                                    isDynamic: true
                                });

                                objRecord2.setValue({
                                    fieldId: 'name',
                                    value: yearint.toString()
                                });
                                objRecord2.setValue({
                                    fieldId: 'parent',
                                    value: arrayCarpetasID[j]
                                });

                                var folderId2 = objRecord2.save({
                                    enableSourcing: true,
                                    ignoreMandatoryFields: true
                                })
                                arrayYear.push(yearint.toString());
                                arrayYearID.push(folderId2)
                            } else {
                                busqueda_carpetas_in.run().each(function (result) {
                                    arrayYear.push(result.getValue({name: 'name'}));
                                    arrayYearID.push(result.getValue({name: 'internalid'}))
                                    return true;
                                });
                                var control2 = true;

                                for (var k = 0; k < arrayYear.length; k++) {
                                    if (arrayYear[k] == yearnow) {
                                        control2 = false;
                                        break;

                                    }
                                }
                                if (control2 == true) {

                                    var yearint3 = parseInt(yearnow);
                                    var objRecord3 = record.create({
                                        type: record.Type.FOLDER,
                                        isDynamic: true
                                    });

                                    objRecord3.setValue({
                                        fieldId: 'name',
                                        value: yearint3.toString()
                                    });
                                    objRecord3.setValue({
                                        fieldId: 'parent',
                                        value: arrayCarpetasID[j]
                                    });

                                    var folderId3 = objRecord3.save({
                                        enableSourcing: true,
                                        ignoreMandatoryFields: true
                                    })
                                    arrayYear.push(yearint3.toString());
                                    arrayYearID.push(folderId3)
                                }

                            }


                        }
                    }
                }
                ObjetoCarpetas.Datos = {
                    carpeta:  arrayYear,
                    carpetaID: arrayYearID
                }
 
                return ObjetoCarpetas;
            }catch (e) {
                log.audit({title:'Error 642', details: e})
            }
        }

        function mes(resyear,yearnow,mesnow,meses) {
            try{
                var ObjetoCarpetas = new Object();
                var arrayMes = [];
                var arrayMesID = [];
                var arrayYear = resyear.Datos.carpeta;
                var arrayYearID= resyear.Datos.carpetaID;
                if(arrayYearID.length>0){
                    for(var  l= 0; l<arrayYearID.length ; l++){
                        if(arrayYear[l]==yearnow){
                            var busqueda_carpetas_mes = search.create({
                                type: search.Type.FOLDER,
                                filters: [
                                    ['parent', search.Operator.IS, arrayYearID[l]],
                                ],
                                columns: [
                                    search.createColumn({name: 'internalid'}),
                                    search.createColumn({name: 'name'}),
                                ]
                            });
                            var searchResultCountmes = busqueda_carpetas_mes.runPaged().count;
                            busqueda_carpetas_mes.run().each(function(result){
                                arrayMes.push(result.getValue({name: 'name'}));

                                arrayMesID.push(result.getValue({name: 'internalid'}))
                                return true;
                            });

                            if(searchResultCountmes<1){
                                var mesint = parseInt(mesnow);
                                var objRecord4 = record.create({
                                    type: record.Type.FOLDER,
                                    isDynamic: true
                                });

                                objRecord4.setValue({
                                    fieldId: 'name',
                                    value: meses[mesint - 1]
                                });
                                objRecord4.setValue({
                                    fieldId: 'parent',
                                    value: arrayYearID[l]
                                });

                                var folderId4 = objRecord4.save({
                                    enableSourcing: true,
                                    ignoreMandatoryFields: true
                                })
                                arrayMes.push(meses[mesint - 1]);
                                arrayMesID.push(folderId4)
                            }else {

                                var control3 = true;
                                for( var m =0;m<arrayMes.length;m++){
                                    if(arrayMes[m]==meses[mesnow-1]){
                                        control3 = false;
                                        break;

                                    }
                                }
                                if(control3==true){

                                    var mesint5 = parseInt(mesnow);
                                    var objRecord5 = record.create({
                                        type: record.Type.FOLDER,
                                        isDynamic: true
                                    });

                                    objRecord5.setValue({
                                        fieldId: 'name',
                                        value: meses[mesint5 -1]
                                    });
                                    objRecord5.setValue({
                                        fieldId: 'parent',
                                        value: arrayYearID[l]
                                    });

                                    var folderId5 = objRecord5.save({
                                        enableSourcing: true,
                                        ignoreMandatoryFields: true
                                    })
                                    arrayMes.push(meses[mesint5 - 1]);
                                    arrayMesID.push(folderId5)
                                }

                            }
                        }


                    }
                }

                ObjetoCarpetas.Datos = {
                    carpeta:  arrayMes,
                    carpetaID: arrayMesID
                }

                return ObjetoCarpetas;
            }catch (e) {
                log.audit({title: 'arrayMes error', details: e });
            }
        }
        function dia(resmes,dias,daynow,dayweeknow,mesnow,meses) {
            try {
                var arrayMes = resmes.Datos.carpeta;
                var arrayMesID = resmes.Datos.carpetaID;
                var arrayDay =[];
                var arrayDayID=[];
                var carpeta = [];
                var carpetaID = [];

                if(arrayMesID.length>0){
                    for(var  n= 0; n<arrayMesID.length ; n++){
                        if(arrayMes[n]==meses[mesnow-1]){
                            var busqueda_carpetas_dia = search.create({
                                type: search.Type.FOLDER,
                                filters: [
                                    ['parent', search.Operator.IS, arrayMesID[n]],
                                ],
                                columns: [
                                    search.createColumn({name: 'internalid'}),
                                    search.createColumn({name: 'name'}),
                                ]
                            });
                            var searchResultCountdia = busqueda_carpetas_dia.runPaged().count;
                            if(searchResultCountdia<1){
                                var dayint = parseInt(daynow);
                                var objRecord6 = record.create({
                                    type: record.Type.FOLDER,
                                    isDynamic: true
                                });

                                objRecord6.setValue({
                                    fieldId: 'name',
                                    value: dias[dayweeknow] +' '+ dayint
                                });
                                objRecord6.setValue({
                                    fieldId: 'parent',
                                    value: arrayMesID[n]
                                });

                                var folderId6 = objRecord6.save({
                                    enableSourcing: true,
                                    ignoreMandatoryFields: true
                                })
                                carpeta.push(dias[dayweeknow] +' '+ dayint);
                                carpetaID.push(folderId6)
                            }else {
                                var dayint = parseInt(daynow);
                                busqueda_carpetas_dia.run().each(function(result){
                                    arrayDay.push(result.getValue({name: 'name'}));
                                    arrayDayID.push(result.getValue({name: 'internalid'}))
                                    return true;
                                });
                                var control4 = true;

                                for( var o =0;o<arrayDay.length;o++){

                                    if(arrayDay[o]== (dias[dayweeknow] +' '+ dayint)){
                                        log.audit({title: 'entro',details: o});
                                        control4 = false;
                                        break;

                                    }
                                }
                                if(control4==true){

                                    var dayint7 = parseInt(daynow);
                                    var objRecord7 = record.create({
                                        type: record.Type.FOLDER,
                                        isDynamic: true
                                    });

                                    objRecord7.setValue({
                                        fieldId: 'name',
                                        value: dias[dayweeknow] +' '+ dayint7
                                    });
                                    objRecord7.setValue({
                                        fieldId: 'parent',
                                        value: arrayMesID[n]
                                    });

                                    var folderId7 = objRecord7.save({
                                        enableSourcing: true,
                                        ignoreMandatoryFields: true
                                    })
                                    carpeta.push(dias[dayweeknow] +' '+ dayint7);
                                    carpetaID.push(folderId7)
                                }

                            }
                        }


                    }
                }
                var ObjetoCarpetas = new Object();
                ObjetoCarpetas.Datos = {
                    carpeta:  carpeta,
                    carpetaID: carpetaID
                }

                return ObjetoCarpetas;


            }catch (e) {
                log.audit({title: 'Error 880', details: e});
            }
        }

        function tipoEmpleadofun(restrans,tipoEmpleado,transaccion) {
            try {
                var ObjetoCarpetas = new Object();
                var arrayYear = [];
                var arrayYearID = [];
                var arrayCarpetas = restrans.Datos.carpeta;
                var arrayCarpetasID= restrans.Datos.carpetaID;
                if(arrayCarpetasID.length>0) {
                    for (var j = 0; j < arrayCarpetasID.length; j++) {
                        if (transaccion == arrayCarpetas[j]) {
                            var busqueda_carpetas_in = search.create({
                                type: search.Type.FOLDER,
                                filters: [
                                    ['parent', search.Operator.IS, arrayCarpetasID[j]],
                                ],
                                columns: [
                                    search.createColumn({name: 'internalid'}),
                                    search.createColumn({name: 'name'}),
                                ]
                            });
                            var searchResultCount = busqueda_carpetas_in.runPaged().count;
                            if (searchResultCount < 1) {

                                var objRecord2 = record.create({
                                    type: record.Type.FOLDER,
                                    isDynamic: true
                                });

                                objRecord2.setValue({
                                    fieldId: 'name',
                                    value: tipoEmpleado
                                });
                                objRecord2.setValue({
                                    fieldId: 'parent',
                                    value: arrayCarpetasID[j]
                                });
                                var folderId2 = objRecord2.save({
                                    enableSourcing: true,
                                    ignoreMandatoryFields: true
                                })
                                arrayYear.push(tipoEmpleado);
                                arrayYearID.push(folderId2)
                            } else {

                                busqueda_carpetas_in.run().each(function (result) {
                                    arrayYear.push(result.getValue({name: 'name'}));
                                    arrayYearID.push(result.getValue({name: 'internalid'}))
                                    return true;
                                });
                                var control2 = true;
                                for (var k = 0; k < arrayYear.length; k++) {
                                    if (arrayYear[k] == tipoEmpleado) {
                                        control2 = false;
                                        break;

                                    }
                                }
                                if (control2 == true) {


                                    var objRecord3 = record.create({
                                        type: record.Type.FOLDER,
                                        isDynamic: true
                                    });

                                    objRecord3.setValue({
                                        fieldId: 'name',
                                        value: tipoEmpleado
                                    });
                                    objRecord3.setValue({
                                        fieldId: 'parent',
                                        value: arrayCarpetasID[j]
                                    });

                                    var folderId3 = objRecord3.save({
                                        enableSourcing: true,
                                        ignoreMandatoryFields: true
                                    })
                                    arrayYear.push(tipoEmpleado);
                                    arrayYearID.push(folderId3)
                                }

                            }


                        }
                    }
                }
                ObjetoCarpetas.Datos = {
                    carpeta:  arrayYear,
                    carpetaID: arrayYearID
                }
                return ObjetoCarpetas;
            }catch (e) {
                log.audit({title: 'Error 993', details: e });
            }
        }

        function tipoSubsidiaria(resTipo,tipoEmpleado,Subsidiaria) {
            try {
                var ObjetoCarpetas = new Object();
                var arrayMes = [];
                var arrayMesID = [];
                var arrayYear = resTipo.Datos.carpeta;
                var arrayYearID= resTipo.Datos.carpetaID;
                if(arrayYearID.length>0){
                    for(var  l= 0; l<arrayYearID.length ; l++){
                        if(arrayYear[l]==tipoEmpleado){
                            var busqueda_carpetas_mes = search.create({
                                type: search.Type.FOLDER,
                                filters: [
                                    ['parent', search.Operator.IS, arrayYearID[l]],
                                ],
                                columns: [
                                    search.createColumn({name: 'internalid'}),
                                    search.createColumn({name: 'name'}),
                                ]
                            });
                            var searchResultCountmes = busqueda_carpetas_mes.runPaged().count;
                            busqueda_carpetas_mes.run().each(function(result){
                                arrayMes.push(result.getValue({name: 'name'}));

                                arrayMesID.push(result.getValue({name: 'internalid'}))
                                return true;
                            });

                            if(searchResultCountmes<1){

                                var objRecord4 = record.create({
                                    type: record.Type.FOLDER,
                                    isDynamic: true
                                });

                                objRecord4.setValue({
                                    fieldId: 'name',
                                    value: Subsidiaria
                                });
                                objRecord4.setValue({
                                    fieldId: 'parent',
                                    value: arrayYearID[l]
                                });

                                var folderId4 = objRecord4.save({
                                    enableSourcing: true,
                                    ignoreMandatoryFields: true
                                })
                                arrayMes.push(Subsidiaria);
                                arrayMesID.push(folderId4)
                            }else {

                                var control3 = true;

                                for( var m =0;m<arrayMes.length;m++){


                                    var x = arrayMes[m];
                                    var y = Subsidiaria;
                                    if(x.length < 23){   var w = 'ini' }
                                    else{
                                        var w =x.substr(23,5)
                                    }
                                    if(y.length < 23){  var z = 'ini'  }
                                    else{
                                        var z =y.substr(23,5)
                                    }


                                    log.audit({title:'w',details: w})
                                    log.audit({title:'z',details: z})
                                    if (w==z){

                                        control3 = false;
                                        break;

                                    }
                                }
                                if(control3==true){


                                    var objRecord5 = record.create({
                                        type: record.Type.FOLDER,
                                        isDynamic: true
                                    });

                                    objRecord5.setValue({
                                        fieldId: 'name',
                                        value: Subsidiaria
                                    });
                                    objRecord5.setValue({
                                        fieldId: 'parent',
                                        value: arrayYearID[l]
                                    });

                                    var folderId5 = objRecord5.save({
                                        enableSourcing: true,
                                        ignoreMandatoryFields: true
                                    })
                                    arrayMes.push(Subsidiaria);
                                    arrayMesID.push(folderId5)
                                }

                            }
                        }


                    }
                }

                ObjetoCarpetas.Datos = {
                    carpeta:  arrayMes,
                    carpetaID: arrayMesID
                }
                return ObjetoCarpetas;
            }catch (e) {
                log.audit({title: 'Error 1123 ', details: e});
            }
        }

        function tipoSubsidiaria2(resTipo,tipoEmpleado,transaccion,Subsidiaria) {
            try {
                var ObjetoCarpetas = new Object();
                var arrayMes = [];
                var arrayMesID = [];
                var arrayYear = resTipo.Datos.carpeta;
                var arrayYearID= resTipo.Datos.carpetaID;
                log.audit("resTipo.Datos.carpeta",arrayYear);
                log.audit("resTipo.Datos.carpetaID",arrayYearID);
                log.audit("transaccion",transaccion);
                log.audit("Subsidiaria",Subsidiaria);
                if(arrayYearID.length>0){
                    for(var  l= 0; l<arrayYearID.length ; l++){
                        if(arrayYear[l]==transaccion){
                            var busqueda_carpetas_mes = search.create({
                                type: search.Type.FOLDER,
                                filters: [
                                    ['parent', search.Operator.IS, arrayYearID[l]],
                                ],
                                columns: [
                                    search.createColumn({name: 'internalid'}),
                                    search.createColumn({name: 'name'}),
                                ]
                            });
                            var searchResultCountmes = busqueda_carpetas_mes.runPaged().count;
                            busqueda_carpetas_mes.run().each(function(result){
                                arrayMes.push(result.getValue({name: 'name'}));

                                arrayMesID.push(result.getValue({name: 'internalid'}))
                                return true;
                            });

                            if(searchResultCountmes<1){

                                var objRecord4 = record.create({
                                    type: record.Type.FOLDER,
                                    isDynamic: true
                                });

                                objRecord4.setValue({
                                    fieldId: 'name',
                                    value: Subsidiaria
                                });
                                objRecord4.setValue({
                                    fieldId: 'parent',
                                    value: arrayYearID[l]
                                });

                                var folderId4 = objRecord4.save({
                                    enableSourcing: true,
                                    ignoreMandatoryFields: true
                                })
                                arrayMes.push(Subsidiaria);
                                arrayMesID.push(folderId4)
                            }else {

                                var control3 = true;

                                for( var m =0;m<arrayMes.length;m++){


                                    var x = arrayMes[m];
                                    var y = Subsidiaria;
                                    if(x.length < 23){   var w = 'ini' }
                                    else{
                                        var w =x.substr(23,5)
                                    }
                                    if(y.length < 23){  var z = 'ini'  }
                                    else{
                                        var z =y.substr(23,5)
                                    }


                                    log.audit({title:'w',details: w})
                                    log.audit({title:'z',details: z})
                                    if (w==z){

                                        control3 = false;
                                        break;

                                    }
                                }
                                if(control3==true){


                                    var objRecord5 = record.create({
                                        type: record.Type.FOLDER,
                                        isDynamic: true
                                    });

                                    objRecord5.setValue({
                                        fieldId: 'name',
                                        value: Subsidiaria
                                    });
                                    objRecord5.setValue({
                                        fieldId: 'parent',
                                        value: arrayYearID[l]
                                    });

                                    var folderId5 = objRecord5.save({
                                        enableSourcing: true,
                                        ignoreMandatoryFields: true
                                    })
                                    arrayMes.push(Subsidiaria);
                                    arrayMesID.push(folderId5)
                                }

                            }
                        }


                    }
                }
                log.audit({title: 'Subsidiarias carpetas', details: arrayMes });
                log.audit({title: 'Subsidiarias carpetasID', details: arrayMesID });
                ObjetoCarpetas.Datos = {
                    carpeta:  arrayMes,
                    carpetaID: arrayMesID
                }
                return ObjetoCarpetas;
            }catch (e) {
                log.audit(">>>>>>>>><tipoSubsidiaria2",  e);
            }
        }

        function nombreEmpleadofun(resSubsi,nombreEmpleado,Subsidiaria) {
            try {
                var ObjetoCarpetas = new Object();
                var arrayMes = [];
                var arrayMesID = [];
                var arrayYear = resSubsi.Datos.carpeta;
                var arrayYearID= resSubsi.Datos.carpetaID;
                if(arrayYearID.length>0){
                    for(var  l= 0; l<arrayYearID.length ; l++){
                        log.audit({title:'l',details: l})
                        var x = arrayYear[l];
                        var y = Subsidiaria;
                        if(x.length < 23){   var w = 'ini' }
                        else{
                            var w =x.substr(23,5)
                        }
                        if(y.length < 23){  var z = 'ini'  }
                        else{
                            var z =y.substr(23,5)
                        }


                        log.audit({title:'w',details: w})
                        log.audit({title:'z',details: z})
                        if (w==z) {
                            var busqueda_carpetas_mes = search.create({
                                type: search.Type.FOLDER,
                                filters: [
                                    ['parent', search.Operator.IS, arrayYearID[l]],
                                ],
                                columns: [
                                    search.createColumn({name: 'internalid'}),
                                    search.createColumn({name: 'name'}),
                                ]
                            });
                            var searchResultCountmes = busqueda_carpetas_mes.runPaged().count;
                            busqueda_carpetas_mes.run().each(function(result){
                                arrayMes.push(result.getValue({name: 'name'}));

                                arrayMesID.push(result.getValue({name: 'internalid'}))
                                return true;
                            });

                            if(searchResultCountmes<1){

                                var objRecord4 = record.create({
                                    type: record.Type.FOLDER,
                                    isDynamic: true
                                });

                                objRecord4.setValue({
                                    fieldId: 'name',
                                    value: nombreEmpleado
                                });
                                objRecord4.setValue({
                                    fieldId: 'parent',
                                    value: arrayYearID[l]
                                });

                                var folderId4 = objRecord4.save({
                                    enableSourcing: true,
                                    ignoreMandatoryFields: true
                                })

                                arrayMes.push(nombreEmpleado);
                                arrayMesID.push(folderId4)
                            }else {

                                var control3 = true;

                                for( var m =0;m<arrayMes.length;m++){
                                    if(arrayMes[m]==nombreEmpleado){

                                        control3 = false;
                                        break;

                                    }
                                }
                                if(control3==true){


                                    var objRecord5 = record.create({
                                        type: record.Type.FOLDER,
                                        isDynamic: true
                                    });

                                    objRecord5.setValue({
                                        fieldId: 'name',
                                        value: nombreEmpleado
                                    });
                                    objRecord5.setValue({
                                        fieldId: 'parent',
                                        value: arrayYearID[l]
                                    });

                                    var folderId5 = objRecord5.save({
                                        enableSourcing: true,
                                        ignoreMandatoryFields: true
                                    })
                                    // log.audit({title: 'folderId5', details: folderId5});
                                    arrayMes.push(nombreEmpleado);
                                    arrayMesID.push(folderId5)
                                }

                            }
                        }


                    }
                }
                //log.audit({title: 'arrayMes', details: arrayMes });
                // log.audit({title: 'arrayMesID', details: arrayMesID });
                ObjetoCarpetas.Datos = {
                    carpeta:  arrayMes,
                    carpetaID: arrayMesID
                }

                return ObjetoCarpetas;
            }catch (e) {
                log.audit({title: 'Error 1386', details: e});
            }
        }

        function archivador(FileID,Folder ){
            try{

                var arrayFile = Folder.Datos.carpeta;
                var arrayFileID= Folder.Datos.carpetaID;


                if(arrayFileID.length>0){
                    for(var i =0 ; i<arrayFileID.length ; i++ ){
                        log.audit({title: 'FileID', details: FileID});
                        var fileObj = file.load({
                            id: FileID
                        });
                        var idFo = parseInt(arrayFileID[i])
                        var idFi = parseInt(FileID)
                        log.audit({title: 'idFo', details: idFo});
                        log.audit({title: 'idFi', details: idFi});
                        fileObj = file.copy({
                            id: idFi,
                            folder: idFo,
                            conflictResolution: file.NameConflictResolution.OVERWRITE
                        });

                    }
                }


            }catch (e) {

            }
        }

        const summarize = (summaryContext) => {

        }

        return {getInputData, map, reduce, summarize}

    });
