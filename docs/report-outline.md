# Xây dựng kho dữ liệu cho hệ thống multi-chain

---

## Mục lục

[Tóm tắt đồ án](#tóm-tắt-đồ-án)

I. [Kho dữ liệu và cách thức triển khai](#i-kho-dữ-liệu-và-cách-thức-triển-khai)

1. [Giới thiệu về kho dữ liệu](#1-giới-thiệu-về-kho-dữ-liệu)

    1.1 [Khái niệm](#11-khái-niệm)

    1.2 [Các đặc tính của kho dữ liệu](#12-các-đặc-tính-của-kho-dữ-liệu)
<!-- 
    1.3 [Phân biệt với hệ thống xử lý giao dịch trực tuyến (OLTP)](#13-phân-biệt-với-hệ-thống-xử-lý-giao-dịch-trực-tuyến-oltp) -->

[II. Bài toán phân tích dữ liệu hệ thống Multi-chain](#ii-bài-toán-phân-tích-dữ-liệu-hệ-thống-multi-chain)

[III. Triển khai giải pháp và áp dụng](#iii-triển-khai-giải-pháp-và-áp-dụng)

---

## Tóm tắt đồ án

## I. Kho dữ liệu và cách thức triển khai

### 1. Giới thiệu về kho dữ liệu

#### 1.1. Khái niệm

Kho dữ liệu (*Data Warehouse*) là một loại hệ thống quản lý dữ liệu được thiết kế để hỗ trợ các hoạt động kinh doanh thông minh (*Business Intelligence*), đặc biệt là phân tích dữ liệu.

Kho dữ liệu được sử dụng để giải quyết nhu cầu thực hiện các truy vấn và phân tích một lượng lớn dữ liệu lịch sử. Chẳng hạn như một cửa hàng bán quần áo muốn biết doanh thu trong từng tháng tiến triển ra sao, mẫu hàng nào có được doanh số tốt trong tháng vừa qua hay đánh giá mức độ thân thiết của các khách hàng từng mua. Hay là một nhà marketing muốn đánh giá và dự báo xu hướng mới để nắm bắt và đưa lên những ý tưởng mới cho đội ngũ sản xuất... Để làm được những công việc đó cần thực hiện truy xuất một lượng lớn dữ liệu có được trong quá khứ và kho dữ liệu là một giải pháp đáp ứng được những nhu cầu đó.

Dữ liệu trong kho dữ liệu thường được lấy từ nhiều nguồn khác nhau như tệp nhật ký ứng dụng và ứng dụng giao dịch,... sau đó được hợp nhất lại với nhau tạo nên một thể thống nhất. Khả năng phân tích của hệ thống cho phép các tổ chức rút ra những hiểu biết kinh doanh có giá trị từ dữ liệu của họ để cải thiện việc ra quyết định. Theo thời gian, kho dữ liệu xây dựng một bản ghi lịch sử đầy giá trị đối với các nhà khoa học dữ liệu và nhà phân tích kinh doanh.

#### 1.2. Các đặc tính của kho dữ liệu

Theo *William H. Inmon* - kiến trúc sư hàng đầu trong việc xây dựng hệ thống kho dữ liệu, đã đưa ra định nghĩa chuyên sâu và rõ ràng hơn về kho dữ liệu rằng đây là một bộ các dữ liệu hướng đối tượng (***subject-oriented***), tích hợp (***integrated***), phụ thuộc theo thời gian (***time-variant***) và không bị thay đổi (***nonvolatile***) để  hỗ  trợ cho quá trình đưa ra quyết định. Một định nghĩa ngắn gọn nhưng chứa trọn bốn điểm nhấn làm rõ sự khác biệt giữa một hệ thống kho dữ liệu và một hệ thống lưu trữ thông thường - một hệ cơ sở dữ liệu quan hệ, các hệ thống giao dịch, hệ thống file lưu trữ,...

* **Hướng đối tượng** - *Subject-oriented*: Với bài toán chính là phân tích, kho dữ liệu được tổng hợp và tổ chức xoay quanh các chủ đề  chính như khách hàng, sản phẩm, các nhà cung ứng, doanh thu. Thay vì chú trọng vào quá trình xử lý các giao dịch của tổ chức, kho dữ liệu tập trung vào quá trình tạo lập các mô hình và phân tích dữ liệu cho các nhà phân tích. Vì vậy, kho dữ liệu thường cung cấp những cái nhìn có chiều sâu vào vào các vấn đề cụ thể nhờ sự chọn lọc những dữ liệu cần thiết cho quá trình ra quyết định.

* **Tích hợp** - *Integrated*: Kho dữ liệu thường được xây dựng bằng cách tích hợp nhiều nguồn không đồng nhất, chẳng hạn như cơ sở dữ liệu quan hệ, các file và bản ghi giao dịch trực tuyến. Cũng vì thế  mà các kỹ thuật làm sạch và tích hợp dữ liệu được áp dụng để đảm bảo tính nhất quán trong quy ước đặt tên, cấu trúc mã hóa, thước đo thuộc tính,...

* **Phụ thuộc thời gian** - *Time-variant*: Dữ liệu được lưu trữ sẽ cung cấp những tri thức theo góc độ thời gian như 5 năm kể từ ngày bán hàng, tình hình tổ chức thường niên, các quý,... đem lại một cái nhìn toàn diện về những khoảng thời gian mà nhà phân tích mong muốn.

* **Không thay đổi** - *Nonvolatile*: Kho dữ liệu sẽ luôn là một nơi chứa riêng biệt về mặt vật lý được chuyển đổi từ mọi dữ liệu tiếp nhận được từ nhiều nguồn. Do sự tách biệt này nên các cơ chế xử lý giao dịch, khôi phục và kiểm soát đồng thời trong kho dữ liệu không phải là yêu cầu bắt buộc. Nó thường chỉ yêu cầu hai thao tác truy cập dữ liệu: thêm mới dữ liệu và truy cập dữ liệu.

Tựu chung lại, xây dựng kho dữ liệu tức áp dụng một kiểu kiến trúc lưu trữ dữ liệu từ nhiều nguồn một cách nhất quán để triển khai hạ tầng vật lý cho mô hình dữ liệu nhằm đáp ứng nhu cầu phân tích, báo cáo và hỗ trợ ra quyết định cho tổ chức, doanh nghiệp.

<!-- #### 1.3. Phân biệt với hệ thống xử lý giao dịch trực tuyến (OLTP)

Các ứng dụng mà ta thường dùng ngày nay như đặt xe, ngân hàng, đặt bàn ở nhà hàng,... đều đã đi vào cuộc sống hàng ngày và trở thành những dịch vụ trực tuyến hữu ích, đáp ứng đủ những nhu cầu về mặt tiêu dùng cho người sử dụng. Để các ứng dụng hoạt động trơn tru đều một phần đến từ việc xây dựng và ứng dụng tốt các hệ thống quản lý cơ sở dữ liệu hoạt động.

Hệ thống quản lý cơ sở dữ liệu hoạt động (*Operational Database System*) là hệ thống được sử dụng để thực hiện xử lý các giao dịch trực tuyến và truy vấn. Các giao dịch trực tuyến để hình dung thì có thể được minh họa thông qua các ví dụ tương tác với ứng dụng được nêu ở trên. Mỗi một lần đặt xe, hệ thống đã phát sinh thêm một giao dịch trực tuyến và cần được xử lý tốt để đảm bảo tính nhất quán và toàn vẹn dữ liệu cho toàn bộ dữ liệu. Một cách nói khác, các hệ thống quản lý cơ sở dữ liệu hoạt động còn được gọi là một hệ thống OLTP (*Online Transaction Processing*) -->




### 2. Mô hình xây dựng kho dữ liệu

### 3. 

## II. Bài toán phân tích dữ liệu hệ thống Multi-chain

## III. Triển khai giải pháp và áp dụng

## Tài liệu tham khảo

[Oracle, What is a data warehouse (oracle1)](https://www.oracle.com/database/what-is-a-data-warehouse/)

[W. H. Inmon. Building the Data Warehouse. John Wiley & Sons, 1996 (inm96)]()