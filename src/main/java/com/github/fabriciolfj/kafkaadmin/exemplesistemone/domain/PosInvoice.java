package com.github.fabriciolfj.kafkaadmin.exemplesistemone.domain;


import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "InvoiceNumber",
        "CreatedTime",
        "StoreID",
        "PosID",
        "CashierID",
        "CustomerType",
        "CustomerCardNo",
        "TotalAmount",
        "NumberOfItems",
        "PaymentMethod",
        "TaxableAmount",
        "CGST",
        "SGST",
        "CESS",
        "DeliveryType",
        "DeliveryAddress",
        "InvoiceLineItems"
})
@Data
public class PosInvoice {

    @JsonProperty("InvoiceNumber")
    private String invoiceNumber;
    @JsonProperty("CreatedTime")
    private Long createdTime;
    @JsonProperty("StoreID")
    private String storeID;
    @JsonProperty("PosID")
    private String posID;
    @JsonProperty("CashierID")
    private String cashierID;
    @JsonProperty("CustomerType")
    private String customerType;
    @JsonProperty("CustomerCardNo")
    private String customerCardNo;
    @JsonProperty("TotalAmount")
    private Double totalAmount;
    @JsonProperty("NumberOfItems")
    private Integer numberOfItems;
    @JsonProperty("PaymentMethod")
    private String paymentMethod;
    @JsonProperty("TaxableAmount")
    private Double taxableAmount;
    @JsonProperty("CGST")
    private Double cGST;
    @JsonProperty("SGST")
    private Double sGST;
    @JsonProperty("CESS")
    private Double cESS;
    @JsonProperty("DeliveryType")
    private String deliveryType;
    @JsonProperty("DeliveryAddress")
    private DeliveryAddress deliveryAddress;
    @JsonProperty("InvoiceLineItems")
    private List<LineItem> invoiceLineItems = new ArrayList<LineItem>();
}
