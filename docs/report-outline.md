# Xây dựng kho dữ liệu cho hệ thống multi-chain

---

## Mục lục

[Tóm tắt đồ án](#tóm-tắt-đồ-án)

[I. Kho dữ liệu và cách thức triển khai](#i-kho-dữ-liệu-và-cách-thức-triển-khai)

[1. Giới thiệu về  kho dữ liệu](#1-giới-thiệu-về-kho-dữ-liệu)

[II. Bài toán phân tích dữ liệu hệ thống Multi-chain](#ii-bài-toán-phân-tích-dữ-liệu-hệ-thống-multi-chain)

[III. Triển khai giải pháp và áp dụng](#iii-triển-khai-giải-pháp-và-áp-dụng)

---

## Tóm tắt đồ án

## I. Kho dữ liệu và cách thức triển khai

### 1. Giới thiệu về kho dữ liệu

Kho dữ liệu (*Data Warehouse*) là một loại hệ thống quản lý dữ liệu được thiết kế để hỗ trợ các hoạt động kinh doanh thông minh (*Business Intelligence*), đặc biệt là phân tích dữ liệu.

Kho dữ liệu được sử dụng khi ta có mục đích thực hiện các truy vấn và phân tích một lượng lớn dữ liệu lịch sử. Dữ liệu trong kho dữ liệu thường được lấy từ nhiều nguồn khác nhau như tệp nhật ký ứng dụng và ứng dụng giao dịch,... sau đó được hợp nhất lại với nhau tạo nên một thể thống nhất. Khả năng phân tích của hệ thống cho phép các tổ chức rút ra những hiểu biết kinh doanh có giá trị từ dữ liệu của họ để cải thiện việc ra quyết định. Theo thời gian, kho dữ liệu xây dựng một bản ghi lịch sử đầy giá trị đối với các nhà khoa học dữ liệu và nhà phân tích kinh doanh.

Theo *William H. Inmon* - kiến trúc sư hàng đầu trong việc xây dựng hệ thống kho dữ liệu, đã đưa ra định nghĩa chuyên sâu và rõ ràng hơn về kho dữ liệu rằng đây là một bộ các dữ liệu hướng đối tượng (***subject-oriented***), tích hợp (***integrated***), phụ thuộc theo thời gian (***time-variant***) và không bị thay đổi (***nonvolatile***) để  hỗ  trợ cho quá trình đưa ra quyết định. Một định nghĩa ngắn gọn nhưng chứa trọn bốn điểm nhấn làm rõ sự khác biệt giữa một hệ thống kho dữ liệu và một hệ thống lưu trữ thông thường - một hệ cơ sở dữ liệu quan hệ, các hệ thống giao dịch, hệ thống file lưu trữ,...

* **Hướng đối tượng** - *Subject-oriented*: Với mong muốn chính là phân tích, kho dữ liệu được tổng hợp và tổ chức xoay quanh các chủ đề  chính như khách hàng, sản phẩm, các nhà cung ứng, doanh thu. Thay vì chú trọng vào quá trình xử lý các giao dịch của tổ chức, kho dữ liệu tập trung vào quá trình tạo lập các mô hình và phân tích dữ liệu cho các nhà phân tích. Vì vậy, kho dữ liệu thường cung cấp những cái nhìn có chiều sâu vào vào các vấn đề cụ thể nhờ sự chọn lọc những dữ liệu cần thiết cho quá trình ra quyết định.

* **Tích hợp** - *Integrated*: Kho dữ liệu thường được xây dựng bằng cách tích hợp nhiều nguồn không đồng nhất, chẳng hạn như cơ sở dữ liệu quan hệ, các file và bản ghi giao dịch trực tuyến. Cũng vì thế  mà các kỹ thuật làm sạch và tích hợp dữ liệu được áp dụng để đảm bảo tính nhất quán trong quy ước đặt tên, cấu trúc mã hóa, thước đo thuộc tính,...

* **Phụ thuộc thời gian** - *Time-variant*: Dữ liệu được lưu trữ sẽ cung cấp những tri thức theo góc độ thời gian như 5 năm kể từ ngày bán hàng, tình hình tổ chức thường niên, các quý,... đem lại một cái nhìn toàn diện về những khoảng thời gian mà nhà phân tích mong muốn.

* **Không thay đổi** - *Nonvolatile*: 


## II. Bài toán phân tích dữ liệu hệ thống Multi-chain

## III. Triển khai giải pháp và áp dụng

## Tài liệu tham khảo

[Oracle, What is a data warehouse (oracle1)](https://www.oracle.com/database/what-is-a-data-warehouse/)

[W. H. Inmon. Building the Data Warehouse. John Wiley & Sons, 1996 (inm96)]()