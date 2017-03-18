function Node(data) {
  this.data = data;
}
 /**
* Linked list.
*/
function LinkedList() {
  this.length = 0;
    // this.head = undefined;
    // this.tail = undefined;
}
LinkedList.prototype.pop = function pop() {
  if (!this.head) {
    return undefined;
  }
  this.length--;
  const retVal = this.head.data;
  this.head = this.head.next;
  return retVal;
};
LinkedList.prototype.add = function add(data) {
  if (data === undefined) {
    throw new Error('Cannot insert undefined into linked list');
  }
  if (!this.head) {
    this.head = this.tail = new Node(data);
  } else {
    this.tail = this.tail.next = new Node(data);
  }
  this.length++;
};
module.exports = LinkedList;
